import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import geopandas as gpd
import h3
from shapely.geometry import Polygon
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# 1. TIENDAS EXISTENTES (Entrenamiento)
BB_STORES = [
    {"city": "Madrid", "name": "Fuencarral", "lat": 40.4287, "lon": -3.7020},
    {"city": "Madrid", "name": "Goya", "lat": 40.4256, "lon": -3.6808},
    {"city": "Valencia", "name": "Valencia Centro", "lat": 39.4735, "lon": -0.3725}
]

# 2. CONFIGURACIÓN DE VETOS
MIN_DIST_VETO_METERS = 1500  # Veto Fuerte: 1.5km de radio de exclusión

# 3. PESOS DE VARIABLES (Feature Weights)
# Nota: Quitamos Vacancy del vector principal para evitar ruido en ciudades sin datos.
FEATURE_WEIGHTS = {
    'income_smooth_score': 6.0,     # RENTA: Lo más importante.
    'target_pop_smooth_score': 3.0, # MASA CRÍTICA
    'dist_cafe_score': 1.5,         # LIFESTYLE
    'dist_gym_score': 1.0,
    'dist_shop_score': 1.0,
    'dist_transit_score': 0.8
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula distancia en metros entre dos puntos (Fórmula Harvesine)"""
    R = 6371000 
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def train_model_v4_contrast():
    print("🧠 ENTRENAMIENTO FINAL (V4: CONTRAST BOOSTER & HARD VETO)...")
    engine = create_engine(DB_URL)
    
    # ---------------------------------------------------------
    # 1. CARGA DE DATOS
    # ---------------------------------------------------------
    print("   Cargando Datasets...")
    query = """
    SELECT 
        e.h3_index, 
        COALESCE(e.target_pop_smooth, 0) as target_pop_smooth, 
        COALESCE(e.income_smooth, 0) as income_smooth,
        e.vacancy_smooth,   -- Puede ser NULL fuera de Madrid
        e.health_status,    
        e.street_profile,
        r.dist_cafe, r.dist_gym, r.dist_shop, r.dist_transit,
        r.lat, r.lon, r.city
    FROM retail_hexagons_enriched e
    JOIN retail_hexagons r ON e.h3_index = r.h3_index
    """
    df = pd.read_sql(query, engine)
    
    # Etiquetar Tiendas Entrenamiento
    df['is_blue_banana'] = 0
    for store in BB_STORES:
        h3_idx = h3.geo_to_h3(store['lat'], store['lon'], 9)
        df.loc[df['h3_index'] == h3_idx, 'is_blue_banana'] = 1

    # ---------------------------------------------------------
    # 2. FEATURE ENGINEERING (Pre-Procesado)
    # ---------------------------------------------------------
    print("   Ingeniería de variables...")
    scaler = MinMaxScaler()
    score_cols = []

    # A. Distancias (Logarítmica Inversa: Para que 10m y 50m se diferencien más que 2km y 3km)
    dist_vars = ['dist_cafe', 'dist_gym', 'dist_shop', 'dist_transit']
    for col in dist_vars:
        df[col] = df[col].fillna(10000) # Rellenar nulos con distancia lejana
        score_col = f"{col}_score"
        score_cols.append(score_col)
        # Fórmula: 1 / log(metros + 1) -> Valores cercanos a 1 son muy buenos
        df[score_col] = df[col].apply(lambda x: 1/(np.log1p(x)+1))

    # B. Volumen (Logarítmica: Suavizar picos de riqueza extrema)
    vol_vars = ['target_pop_smooth', 'income_smooth']
    for col in vol_vars:
        score_col = f"{col}_score"
        score_cols.append(score_col)
        df[score_col] = np.log1p(df[col])

    # Escalado 0-1
    df[score_cols] = scaler.fit_transform(df[score_cols])

    # ---------------------------------------------------------
    # 3. ENTRENAMIENTO DEL VECTOR IDEAL
    # ---------------------------------------------------------
    # Aplicar Pesos
    weighted_cols = []
    for col in score_cols:
        weight = FEATURE_WEIGHTS.get(col, 1.0)
        w_col = f"{col}_w"
        df[w_col] = df[col] * weight
        weighted_cols.append(w_col)

    train_df = df[df['is_blue_banana'] == 1]
    if train_df.empty:
        print("❌ Error: No se encuentran las tiendas de entrenamiento.")
        return

    # Creamos el "Fantasma Perfecto" (Promedio de tus tiendas)
    ideal_vector = train_df[weighted_cols].mean().values.reshape(1, -1)
    
    # Calculamos Umbral de Renta Mínima (Seguridad)
    min_income_threshold = train_df['income_smooth'].min() * 0.85
    print(f"   -> Umbral Renta (Hard Veto): {min_income_threshold:.0f}€")

    # ---------------------------------------------------------
    # 4. CÁLCULO DE SIMILITUD Y CONTRASTE (SOLUCIÓN POLARIZACIÓN)
    # ---------------------------------------------------------
    print("   Calculando Similitud Coseno...")
    matrix = df[weighted_cols].values
    raw_sim = cosine_similarity(matrix, ideal_vector).flatten()
    
    # --- AQUÍ ESTÁ EL TRUCO DEL CONTRASTE ---
    # Los vectores positivos siempre dan similitud alta (0.8 - 1.0).
    # Vamos a estirar eso para que 0.90 sea un 20/100 y 0.99 sea un 90/100.
    
    print("   🎨 Aplicando 'Contrast Stretching' para diferenciar hexágonos...")
    # Potencia alta (15-20) separa a los "buenos" de los "excelentes"
    # Si raw_sim es 0.95 -> 0.95^20 = 0.35 (Score bajo)
    # Si raw_sim es 0.99 -> 0.99^20 = 0.81 (Score alto)
    contrast_power = 20 
    df['similarity_raw'] = raw_sim
    df['similarity_contrasted'] = np.power(raw_sim, contrast_power) * 100

    # ---------------------------------------------------------
    # 5. VETOS DUROS Y AJUSTE FINAL
    # ---------------------------------------------------------
    print(f"   🛡️  Aplicando Veto de Canibalización ({MIN_DIST_VETO_METERS}m)...")

    def apply_final_logic(row):
        # Empezamos con el score contrastado
        score = row['similarity_contrasted']
        
        # --- A. VETOS DUROS (RETURN 0) ---
        
        # 1. Veto de Renta
        if row['income_smooth'] < min_income_threshold: return 0 
        
        # 2. Veto de Canibalización (1.5km)
        for store in BB_STORES:
            if row['city'] == store['city']:
                dist = haversine_distance(row['lat'], row['lon'], store['lat'], store['lon'])
                if dist < MIN_DIST_VETO_METERS: return 0 # ¡AQUÍ ESTÁ TU 1.5KM!
        
        # 3. Veto de "Calle Muerta" (Solo si tenemos dato)
        if row['health_status'] == 'Distressed (High Risk)': return 0
        
        # --- B. MODIFICADORES SUAVES (Vacancy) ---
        # Si tenemos datos de vacancy (Madrid), lo usamos para subir/bajar nota
        # No mata el score, solo lo modula.
        if pd.notna(row['vacancy_smooth']):
            vac = row['vacancy_smooth']
            if vac >= 0.90: score *= 1.15  # Boost fuerte a zonas muy vivas
            elif vac < 0.60: score *= 0.80 # Penalización a zonas reguleras
        
        # Boost Fashion
        if row['street_profile'] == 'Fashion District':
            score *= 1.10

        return min(100, score) # Capamos a 100

    df['similarity_final'] = df.apply(apply_final_logic, axis=1)

    # ---------------------------------------------------------
    # 6. GUARDADO FINAL
    # ---------------------------------------------------------
    print("💾 Guardando resultados...")
    
    # Crear Geometría
    def get_poly(x): return Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
    df['geom'] = df['h3_index'].apply(get_poly)
    gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")
    
    # Columnas para PostGIS (incluyendo metadatos útiles para debugging)
    final_cols = [
        'h3_index', 'city', 'lat', 'lon', 'geom',
        'similarity_final',      # EL SCORE BUENO
        'similarity_raw',        # EL SCORE MATEMÁTICO (Para que veas la diferencia)
        'target_pop_smooth', 
        'income_smooth', 
        'vacancy_smooth', 
        'street_profile',
        'health_status',
        'dist_cafe'
    ]
    
    gdf[final_cols].to_postgis('retail_results', engine, if_exists='replace', index=False)
    
    print("\n✅ MODELO ACTUALIZADO.")
    print(f"   Regla aplicada: < {MIN_DIST_VETO_METERS/1000}km = Score 0.")
    print("   Polarización corregida con exponente ^20.")

if __name__ == "__main__":
    train_model_v4_contrast()
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import geopandas as gpd
import h3
from shapely.geometry import Polygon
import warnings
import os
import sys

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

warnings.filterwarnings("ignore")

# 1. TIENDAS EXISTENTES (Entrenamiento - Ground Truth)
# Estas son las coordenadas que definen el "Éxito" para Blue Banana
BB_STORES = [
    {"city": "Madrid", "name": "Fuencarral", "lat": 40.4287, "lon": -3.7020},
    {"city": "Madrid", "name": "Goya", "lat": 40.4256, "lon": -3.6808},
    {"city": "Valencia", "name": "Valencia Centro", "lat": 39.4735, "lon": -0.3725}
]

# 2. CONFIGURACIÓN DE VETOS
MIN_DIST_VETO_METERS = 1500  # Veto Fuerte: 1.5km de radio de exclusión
MIN_INCOME_PERCENTILE = 0.85 # Debes tener al menos el 85% de la renta de la peor tienda

# 3. PESOS DE VARIABLES (Feature Weights)
# Aquí es donde le damos personalidad al modelo.
# Blue Banana = Renta Alta + Hipster Vibe + Retail Premium.
FEATURE_WEIGHTS = {
    # Variables DEMOGRÁFICAS (Base sólida)
    'income_smooth_score': 6.0,     # RENTA: Lo más importante. Sin dinero no hay venta.
    'target_pop_smooth_score': 3.0, # MASA CRÍTICA: Necesitamos gente joven cerca.
    
    # Variables LIFESTYLE (El Vibe)
    'score_hipster': 4.0,           # ESENCIAL: Cafés de especialidad, yoga, brunch.
    'score_retail': 3.0,            # ESENCIAL: Boutiques y marcas afines.
    'score_health': 2.0,            # IMPORTANTE: Gente fit.
    'score_night': 1.0,             # MENOS RELEVANTE: Ocio nocturno (ayuda pero no define).
    
    # Variables FÍSICAS (Accesibilidad)
    'dist_transit_score': 1.5       # Metro cerca ayuda.
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula distancia en metros entre dos puntos (Fórmula Haversine)"""
    R = 6371000 
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def train_model_final():
    print("🧠 ENTRENAMIENTO FINAL (V5: LIFESTYLE + AFFINITY)...")
    engine = create_engine(DB_CONNECTION_STR)
    
    # ---------------------------------------------------------
    # 1. CARGA DE DATOS
    # ---------------------------------------------------------
    print("   Cargando Datasets Enriquecidos...")
    # Seleccionamos las columnas que realmente existen tras tu limpieza
    query = """
    SELECT 
        e.h3_index, 
        e.city,
        e.lat, e.lon,
        -- Variables Suavizadas (Contexto)
        COALESCE(e.target_pop_smooth, 0) as target_pop_smooth, 
        COALESCE(e.income_smooth, 0) as income_smooth,
        -- Variables Vibe (Google/Affinity)
        COALESCE(e.score_hipster, 0) as score_hipster,
        COALESCE(e.score_retail, 0) as score_retail,
        COALESCE(e.score_health, 0) as score_health,
        COALESCE(e.score_night, 0) as score_night,
        -- Variables Físicas
        r.dist_transit
    FROM retail_hexagons_enriched e
    JOIN retail_hexagons r ON e.h3_index = r.h3_index
    """
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Error SQL: {e}")
        return

    if df.empty:
        print("❌ Error: Dataset vacío.")
        return
    
    # Etiquetar Tiendas Entrenamiento (Ground Truth)
    df['is_blue_banana'] = 0
    # Guardamos los índices H3 de las tiendas para el veto de canibalización luego
    store_h3_indices = []
    
    print("   Identificando tiendas de entrenamiento...")
    for store in BB_STORES:
        # Buscamos el H3 índice de la coordenada de la tienda
        h3_idx = h3.geo_to_h3(store['lat'], store['lon'], 9)
        store_h3_indices.append(h3_idx)
        
        # Marcamos en el DataFrame
        # Nota: Puede que la coordenada exacta caiga en un hexágono que no está en el DF si OSM recortó raro
        if h3_idx in df['h3_index'].values:
            df.loc[df['h3_index'] == h3_idx, 'is_blue_banana'] = 1
            print(f"      ✅ Tienda encontrada en dataset: {store['name']} ({h3_idx})")
        else:
            print(f"      ⚠️ AVISO: La tienda {store['name']} cae fuera de tu mapa actual.")

    # ---------------------------------------------------------
    # 2. FEATURE ENGINEERING (Transformación)
    # ---------------------------------------------------------
    print("   Ingeniería de variables (Log & Scale)...")
    scaler = MinMaxScaler()
    score_cols = []

    # A. Distancias Físicas (Inversa Logarítmica)
    # Queremos que 'dist_transit' sea alta si está cerca (0m) y baja si está lejos.
    df['dist_transit'] = df['dist_transit'].fillna(10000)
    col = 'dist_transit'
    score_col = f"{col}_score"
    score_cols.append(score_col)
    # 1 / log(metros + 1) -> Cerca = 1.0, Lejos = 0.0
    df[score_col] = df[col].apply(lambda x: 1/(np.log1p(x)+1))

    # B. Variables de Volumen y Vibe (Logarítmica Directa)
    # Queremos suavizar diferencias extremas (Renta 100k vs 20k)
    vol_vars = ['target_pop_smooth', 'income_smooth', 'score_hipster', 'score_retail', 'score_health', 'score_night']
    for col in vol_vars:
        score_col = f"{col}" if 'score_' in col else f"{col}_score"
        # Si ya se llama score_hipster, lo dejamos así para usarlo en FEATURE_WEIGHTS
        # Si es income_smooth, lo llamamos income_smooth_score
        
        if score_col not in score_cols: score_cols.append(score_col)
        
        # Aplicamos Logaritmo
        df[score_col] = np.log1p(df[col])

    # Escalado 0-1 Global
    # Esto pone todas las variables en la misma cancha de juego
    df[score_cols] = scaler.fit_transform(df[score_cols])

    # ---------------------------------------------------------
    # 3. ENTRENAMIENTO DEL VECTOR IDEAL
    # ---------------------------------------------------------
    # Aplicar Pesos de Negocio
    weighted_cols = []
    for col in score_cols:
        # Si la columna está en nuestros pesos, usamos ese peso. Si no, 1.0 (o 0.0 para ignorar)
        # Por defecto, si no está en FEATURE_WEIGHTS, asumimos que no es crítica (peso bajo 0.5)
        weight = FEATURE_WEIGHTS.get(col, 0.5)
        
        w_col = f"{col}_w"
        df[w_col] = df[col] * weight
        weighted_cols.append(w_col)

    train_df = df[df['is_blue_banana'] == 1]
    
    if train_df.empty:
        print("❌ Error Crítico: No hay tiendas de entrenamiento válidas en el mapa. No puedo aprender.")
        print("   -> Solución: Verifica que las coordenadas de BB_STORES caen dentro de tus ciudades procesadas.")
        return

    # Creamos el "Fantasma Perfecto" (Vector Promedio de Éxito)
    ideal_vector = train_df[weighted_cols].mean().values.reshape(1, -1)
    
    # Calculamos Umbral de Renta Mínima (Hard Veto)
    # Cogemos la renta de la tienda más "pobre" de las exitosas y aplicamos el factor
    min_income_val = train_df['income_smooth'].min()
    min_income_threshold = min_income_val * MIN_INCOME_PERCENTILE
    print(f"   -> Aprendido: Vector Ideal basado en {len(train_df)} tiendas.")
    print(f"   -> Regla de Seguridad: Renta Mínima > {min_income_threshold:,.0f}€ (85% de la peor tienda exitosa)")

    # ---------------------------------------------------------
    # 4. CÁLCULO DE SIMILITUD (COSINE SIMILARITY)
    # ---------------------------------------------------------
    print("   Calculando Similitud Coseno & Contrast Stretching...")
    matrix = df[weighted_cols].values
    
    # Similitud pura (-1 a 1, aunque aquí será 0 a 1 porque todo es positivo)
    raw_sim = cosine_similarity(matrix, ideal_vector).flatten()
    df['similarity_raw'] = raw_sim
    
    # Contrast Stretching (Polarización)
    # Elevamos a potencia para separar a los buenos (0.95) de los mediocres (0.80)
    contrast_power = 20 
    df['similarity_contrasted'] = np.power(raw_sim, contrast_power) * 100

    # ---------------------------------------------------------
    # 5. VETOS DUROS Y AJUSTE FINAL
    # ---------------------------------------------------------
    print(f"   🛡️ Aplicando Vetos (Renta & Canibalización {MIN_DIST_VETO_METERS}m)...")

    def apply_final_logic(row):
        score = row['similarity_contrasted']
        
        # --- A. VETOS DUROS (KILL SWITCHES) ---
        
        # 1. Veto de Renta
        # Si el barrio es demasiado pobre comparado con nuestro estándar, fuera.
        if row['income_smooth'] < min_income_threshold: return 0 
        
        # 2. Veto de Canibalización
        # Si está demasiado cerca de una tienda existente, fuera.
        for store in BB_STORES:
            # Solo chequeamos distancia si es la misma ciudad
            # (Limpieza simple de nombre ciudad para comparar "Madrid, Spain" con "Madrid")
            if store['city'].lower() in row['city'].lower():
                dist = haversine_distance(row['lat'], row['lon'], store['lat'], store['lon'])
                if dist < MIN_DIST_VETO_METERS: return 0
        
        # --- B. BONUS EXTRA ---
        # Si tiene mucho Vibe Hipster, le damos un empujoncito final
        # (Esto ayuda a destacar las zonas MUY de moda)
        # row['score_hipster'] está en escala log (no original), así que usamos el raw si queremos
        # o simplemente confiamos en el peso que ya le dimos.
        
        return min(100, score) # Capamos a 100 por estética

    df['similarity_final'] = df.apply(apply_final_logic, axis=1)

    # ---------------------------------------------------------
    # 6. GUARDADO FINAL
    # ---------------------------------------------------------
    print("💾 Guardando resultados en 'retail_results'...")
    
    # Crear Geometría para visualización
    def get_poly(x): 
        try: return Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
        except: return None
        
    df['geom'] = df['h3_index'].apply(get_poly)
    df = df.dropna(subset=['geom']) # Seguridad
    
    gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")
    
    # Selección de columnas finales para el Dashboard
    final_cols = [
        'h3_index', 'city', 'lat', 'lon', 'geom',
        'similarity_final',      # KPI FINAL
        'similarity_raw',        # KPI TÉCNICO
        'target_pop_smooth',     # DATOS CONTEXTO
        'income_smooth', 
        'score_hipster',         # DATOS VIBE
        'score_retail',
        'dist_transit'
    ]
    
    # Guardamos en PostGIS
    gdf[final_cols].to_postgis('retail_results', engine, if_exists='replace', index=False)
    
    # Hacemos la tabla rápida de leer
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_results_geom ON retail_results USING GIST(geom);"))

    print("\n✅ MODELO ACTUALIZADO Y ENTRENADO.")
    
    # Top 3 Candidatos
    print("\n🏆 TOP 3 CANDIDATOS (No vetados):")
    top_candidates = df[df['similarity_final'] > 0].sort_values('similarity_final', ascending=False).head(3)
    for idx, row in top_candidates.iterrows():
        print(f"   📍 {row['city']} (H3: {row['h3_index']}) - Score: {row['similarity_final']:.1f}/100")
        print(f"      Renta: {row['income_smooth']:.0f}€ | Hipster Score (Log): {row['score_hipster']:.2f}")

if __name__ == "__main__":
    train_model_final()
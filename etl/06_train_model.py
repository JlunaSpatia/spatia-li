import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import geopandas as gpd
import h3
from shapely.geometry import Polygon, Point
import warnings

# --- CONFIG ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# TIENDAS EXISTENTES (Lat/Lon) - Esto define el "ADN" y las "Zonas Prohibidas"
BB_STORES = [
    {"city": "Madrid", "name": "Fuencarral", "lat": 40.4287, "lon": -3.7020},
    {"city": "Madrid", "name": "Goya", "lat": 40.4256, "lon": -3.6808},
    {"city": "Valencia", "name": "Valencia Centro", "lat": 39.4735, "lon": -0.3725}
]

# PESOS DE NEGOCIO (Elistismo Extremo)
# Subimos Renta a 5.0 para que domine sobre todo lo demás
FEATURE_WEIGHTS = {
    'income_smooth_score': 5.0,     # RENTA: Factor Dominante
    'target_pop_smooth_score': 2.0, # Target Joven
    'dist_cafe_score': 1.0,
    'dist_gym_score': 1.0,
    'dist_shop_score': 1.0,
    'dist_transit_score': 0.5
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula distancia en metros entre dos puntos (Fórmula Haversine)"""
    R = 6371000 # Radio Tierra en metros
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def train_final_model_hardcore():
    print("🧠 ENTRENAMIENTO FINAL (MODO DESPIADADO)...")
    engine = create_engine(DB_URL)
    
    # 1. CARGAR DATOS
    print("   Cargando Datasets...")
    query = """
    SELECT 
        e.h3_index, e.target_pop_smooth, e.income_smooth, 
        r.dist_cafe, r.dist_gym, r.dist_shop, r.dist_transit,
        r.lat, r.lon, r.city
    FROM retail_hexagons_enriched e
    JOIN retail_hexagons r ON e.h3_index = r.h3_index
    """
    df = pd.read_sql(query, engine)
    
    # 2. DEFINIR TIENDAS (LABELS)
    df['is_blue_banana'] = 0
    for store in BB_STORES:
        h3_idx = h3.geo_to_h3(store['lat'], store['lon'], 9)
        df.loc[df['h3_index'] == h3_idx, 'is_blue_banana'] = 1

    # 3. FEATURE ENGINEERING
    print("   Ingeniería de variables...")
    scaler = MinMaxScaler()
    score_cols = []

    # A. Distancias (Menos es Mejor)
    dist_vars = ['dist_cafe', 'dist_gym', 'dist_shop', 'dist_transit']
    for col in dist_vars:
        df[col] = df[col].fillna(9999)
        score_col = f"{col}_score"
        score_cols.append(score_col)
        df[score_col] = df[col].apply(lambda x: 1/(np.log1p(x)+1))

    # B. Volumen (Más es Mejor)
    # Renta y Target Pop
    vol_vars = ['target_pop_smooth', 'income_smooth']
    for col in vol_vars:
        df[col] = df[col].fillna(0)
        score_col = f"{col}_score"
        score_cols.append(score_col)
        df[score_col] = np.log1p(df[col])

    # Escalado 0-1
    df[score_cols] = scaler.fit_transform(df[score_cols])

    # 4. APLICAR PESOS
    weighted_cols = []
    for col in score_cols:
        weight = FEATURE_WEIGHTS.get(col, 1.0)
        w_col = f"{col}_w"
        df[w_col] = df[col] * weight
        weighted_cols.append(w_col)

    # 5. ENTRENAMIENTO
    train_df = df[df['is_blue_banana'] == 1]
    if train_df.empty: return

    # Vector Ideal
    ideal_vector = train_df[weighted_cols].mean().values.reshape(1, -1)
    
    # Análisis de Umbral de Renta (Basado en las tiendas actuales)
    # Buscamos la tienda "más pobre" que funciona y ponemos el corte ahí.
    min_income_threshold = train_df['income_smooth'].min() * 0.9 # Un 10% de margen hacia abajo
    print(f"\n💰 UMBRAL DE RENTA (GATEKEEPER): {min_income_threshold:.0f}€")
    print(f"   (Cualquier zona por debajo de esto será descartada)")

    # 6. PREDICCIÓN BASE
    print("\n   Calculando Similitud Matemática...")
    matrix = df[weighted_cols].values
    sim_scores = cosine_similarity(matrix, ideal_vector)
    df['similarity'] = sim_scores * 100
    
    # ==============================================================================
    # 🔥 7. APLICACIÓN DE VETOS (HARD FILTERS)
    # ==============================================================================
    print("   🛡️  APLICANDO VETOS (Renta y Canibalización)...")
    
    def apply_hard_filters(row):
        # A. VETO DE POBREZA
        if row['income_smooth'] < min_income_threshold:
            return 0 # Eliminado por renta baja
            
        # B. VETO DE CANIBALIZACIÓN
        # Si está a menos de 800m de una tienda existente EN SU CIUDAD
        for store in BB_STORES:
            if row['city'] == store['city']: # Solo chequeamos canibalización en la misma ciudad
                dist_metros = haversine_distance(row['lat'], row['lon'], store['lat'], store['lon'])
                if dist_metros < 800: # 800 metros de buffer
                    return 0 # Eliminado por canibalización
        
        return row['similarity']

    df['similarity_adjusted'] = df.apply(apply_hard_filters, axis=1)
    
    # Estadísticas de la purga
    purged = len(df[df['similarity'] > 0]) - len(df[df['similarity_adjusted'] > 0])
    print(f"   ☠️  Zonas purgadas por los vetos: {purged}")
    
    # Sobreescribimos el score
    df['similarity'] = df['similarity_adjusted']

    # 8. GUARDAR RESULTADO
    print("💾 Guardando 'retail_results'...")
    
    def get_poly(x): return Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
    df['geom'] = df['h3_index'].apply(get_poly)
    gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")
    
    # Columnas finales
    final_cols = [
        'h3_index', 'city', 'lat', 'lon', 'similarity', 'geom',
        'dist_cafe', 'dist_gym', 'dist_shop',
        'target_pop_smooth', 'income_smooth'
    ]
    
    gdf[final_cols].to_postgis('retail_results', engine, if_exists='replace', index=False)
    
    print("\n🏆 TOP 3 VALENCIA (DESPIADADO):")
    top_val = df[df['city'] == 'Valencia'].sort_values('similarity', ascending=False).head(3)
    print(top_val[['h3_index', 'similarity', 'income_smooth']])

if __name__ == "__main__":
    train_final_model_hardcore()
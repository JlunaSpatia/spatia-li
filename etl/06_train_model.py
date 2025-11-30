import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import geopandas as gpd
import h3
from shapely.geometry import Polygon
import warnings

# --- CONFIG ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# --- 🧠 LÓGICA DE NEGOCIO (PESOS) ---
# Aquí definimos qué importa más para Blue Banana
FEATURE_WEIGHTS = {
    'income_smooth_score': 4.0,   # LA RENTA ES REY (x4 importancia)
    'pop_smooth_score': 2.0,      # La masa crítica importa (x2)
    'gravity_smooth_score': 1.5,  # El ambiente importa (x1.5)
    'dist_cafe_score': 1.0,       # Complementos (x1)
    'dist_gym_score': 1.0,
    'dist_shop_score': 1.0
}

def train_final_model_expert():
    print("🧠 ENTRENAMIENTO 'EXPERT' (PONDERADO)...")
    engine = create_engine(DB_URL)
    
    # 1. CARGAR DATOS
    query = """
    SELECT 
        e.h3_index, e.pop_smooth, e.income_smooth, e.gravity_smooth, 
        r.dist_cafe, r.dist_gym, r.dist_shop, r.lat, r.lon, r.city
    FROM retail_hexagons_enriched e
    JOIN retail_hexagons r ON e.h3_index = r.h3_index
    """
    df = pd.read_sql(query, engine)
    
    # 2. LABELS
    bb_stores = [
        {"lat": 40.4287, "lon": -3.7020}, {"lat": 40.4256, "lon": -3.6808}, {"lat": 39.4735, "lon": -0.3725}
    ]
    df['is_blue_banana'] = 0
    for store in bb_stores:
        h3_idx = h3.geo_to_h3(store['lat'], store['lon'], 9)
        df.loc[df['h3_index'] == h3_idx, 'is_blue_banana'] = 1

    # 3. NORMALIZACIÓN (Score 0-1)
    scaler = MinMaxScaler()
    
    # A. Distancias (Inverso)
    dist_vars = ['dist_cafe', 'dist_gym', 'dist_shop']
    for col in dist_vars:
        df[col] = df[col].fillna(9999)
        # Log inverso más agresivo para penalizar lejanía
        df[f"{col}_score"] = df[col].apply(lambda x: 1/(np.log1p(x)+1))

    # B. Variables Volumen (Directo)
    vol_vars = ['pop_smooth', 'income_smooth', 'gravity_smooth']
    for col in vol_vars:
        df[col] = df[col].fillna(0)
        # Usamos el valor directo normalizado después
        df[f"{col}_score"] = df[col]

    # Escalado MinMax de todas las columnas _score
    score_cols = [f"{c}_score" for c in dist_vars + vol_vars]
    df[score_cols] = scaler.fit_transform(df[score_cols])

    # 4. APLICACIÓN DE PESOS (LA MAGIA) ⚖️
    # Multiplicamos cada columna por su peso de negocio ANTES de calcular similitud
    weighted_cols = []
    for col in score_cols:
        weight = FEATURE_WEIGHTS.get(col, 1.0)
        w_col = f"{col}_w"
        df[w_col] = df[col] * weight
        weighted_cols.append(w_col)
        
    print(f"   Pesos aplicados: {FEATURE_WEIGHTS}")

    # 5. ENTRENAMIENTO (Con vectores ponderados)
    train_df = df[df['is_blue_banana'] == 1]
    if train_df.empty: return

    ideal_vector = train_df[weighted_cols].mean().values.reshape(1, -1)
    
    # 6. PREDICCIÓN
    matrix = df[weighted_cols].values
    sim_scores = cosine_similarity(matrix, ideal_vector)
    df['similarity'] = sim_scores * 100
    
    # 7. EL "GATEKEEPER" (FILTROS DE CALIDAD) 🛑
    # Si la renta es baja, matamos el score, aunque tenga muchos cafés.
    # Umbral: Media de la ciudad - 20% (ajustable) o valor fijo.
    # Vamos a ser duros: Si income_smooth < 28.000 (aprox), penalización severa.
    
    # Análisis de renta de las tiendas actuales
    min_income_stores = train_df['income_smooth'].min()
    print(f"   Renta mínima en tiendas actuales: {min_income_stores:.0f}€")
    
    # Penalización: Si tienes menos del 80% de la renta de la peor tienda BB, tu score baja un 50%
    threshold = min_income_stores * 0.8
    
    def apply_penalty(row):
        score = row['similarity']
        if row['income_smooth'] < threshold:
            return score * 0.4 # Penalización del 60%
        return score

    df['similarity_adjusted'] = df.apply(apply_penalty, axis=1)
    
    # Usamos el ajustado como final
    df['similarity'] = df['similarity_adjusted']

    # 8. GUARDAR
    print("💾 Guardando resultados...")
    
    def get_poly(x): return Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
    df['geom'] = df['h3_index'].apply(get_poly)
    gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")
    
    final_cols = [
        'h3_index', 'city', 'lat', 'lon', 'similarity', 'geom',
        'dist_cafe', 'dist_gym', 'dist_shop',
        'pop_smooth', 'income_smooth', 'gravity_smooth'
    ]
    
    gdf[final_cols].to_postgis('retail_results', engine, if_exists='replace', index=False)
    
    print("🏆 TOP 3 VALENCIA (MODELO SNOB):")
    print(df[df['city'] == 'Valencia'].sort_values('similarity', ascending=False).head(3)[['h3_index', 'similarity', 'income_smooth']])

if __name__ == "__main__":
    train_final_model_expert()
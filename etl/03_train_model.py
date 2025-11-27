import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import geopandas as gpd
import h3
from shapely.geometry import Polygon
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

def train_and_predict():
    print("🧠 INICIANDO MOTOR DE IA - BÚSQUEDA DE GEMELOS...")
    
    engine = create_engine(DB_URL)
    
    # 1. Leer Datos
    print("   Leyendo datos de PostGIS...")
    query = "SELECT * FROM retail_hexagons"
    df = pd.read_sql(query, engine)
    
    # 2. Ingeniería de Features
    features = ['dist_cafe', 'dist_gym', 'dist_shop', 'dist_transit']
    feature_cols_norm = []
    
    scaler = MinMaxScaler()
    
    for col in features:
        new_col = f"{col}_score"
        feature_cols_norm.append(new_col)
        # Score inverso: Si distancia es 0 (muy cerca), score alto.
        df[new_col] = df[col].apply(lambda x: 1/x if x > 0 else 1)
    
    # Escalamos de 0 a 1
    df[feature_cols_norm] = scaler.fit_transform(df[feature_cols_norm])
    
    # 3. ENTRENAMIENTO (MADRID)
    train_df = df[df['is_blue_banana'] == 1]
    
    if train_df.empty:
        print("❌ ERROR CRÍTICO: No hay tiendas de entrenamiento.")
        return

    ideal_vector = train_df[feature_cols_norm].mean().values.reshape(1, -1)
    print(f"🧬 ADN Blue Banana extraído de {len(train_df)} tiendas.")
    
    # 4. PREDICCIÓN (VALENCIA + MADRID)
    print("   Calculando similitud matemática...")
    matrix = df[feature_cols_norm].values
    sim_scores = cosine_similarity(matrix, ideal_vector)
    df['similarity'] = sim_scores * 100 
    
    # 5. GENERAR GEOMETRÍA (FIX: POLÍGONOS REALES)
    print("⬡ Generando geometrías hexagonales...")
    
    def get_hex_poly(h3_idx):
        # Devuelve un objeto Polygon de Shapely
        return Polygon(h3.h3_to_geo_boundary(h3_idx, geo_json=True))

    df['geom'] = df['h3_index'].apply(get_hex_poly)
    
    # Convertir a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geom')
    gdf.set_crs(epsg=4326, inplace=True)
    
    # 6. GUARDAR RESULTADOS (Usando GeoPandas -> PostGIS)
    print("💾 Guardando tabla 'retail_results' en PostGIS...")
    
    # Seleccionamos columnas útiles
    output_cols = ['h3_index', 'city', 'lat', 'lon', 'similarity', 'geom'] + features
    gdf_out = gdf[output_cols]
    
    # to_postgis maneja la creación de la columna geométrica automáticamente
    gdf_out.to_postgis('retail_results', engine, if_exists='replace', index=False)
        
    print("🏆 TOP 3 CANDIDATOS EN VALENCIA:")
    print(df[df['city'] == 'Valencia'].sort_values('similarity', ascending=False).head(3)[['h3_index', 'similarity']])

if __name__ == "__main__":
    train_and_predict()
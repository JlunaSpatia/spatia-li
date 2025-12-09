import pandas as pd
from sqlalchemy import create_engine, text  # <--- IMPORTANTE: Importamos text
import h3
from shapely.geometry import Polygon
import geopandas as gpd
import os
import sys

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR 
except ImportError:
    # Ajusta esto si tu config.py tiene otro nombre de variable o BBDD
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

CSV_PATH = "etl/final_dataset.csv"

def load_data_to_postgis():
    print("🚀 PASO 02: CARGA A POSTGIS (GEOMETRÍA)...")
    
    # 1. Leer el CSV Maestro
    # Usamos ruta absoluta basada en la ubicación del script para evitar errores de "File Not Found"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_full_path = os.path.join(base_dir, "final_dataset.csv")

    print(f"   📥 Leyendo {csv_full_path}...")
    try:
        df = pd.read_csv(csv_full_path)
        print(f"      -> Filas cargadas: {len(df)}")
    except FileNotFoundError:
        print("   ❌ ERROR CRÍTICO: No encuentro el CSV. Ejecuta el paso 01 primero.")
        return

    # 2. Generar Geometría (Polígonos)
    print("   ⬡ Convirtiendo índices H3 a Polígonos Reales...")
    
    def get_hex_polygon(h3_index):
        try:
            boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
            return Polygon(boundary)
        except:
            return None

    df['geometry'] = df['h3_index'].apply(get_hex_polygon)
    df = df.dropna(subset=['geometry'])

    # 3. Convertir a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.set_crs(epsg=4326, inplace=True) 

    # 4. Conexión a Base de Datos
    print(f"   🔌 Conectando a PostGIS...")
    engine = create_engine(DB_CONNECTION_STR)

    # 5. Subida Optimizada
    TABLE_NAME = 'retail_hexagons'
    print(f"   💾 Escribiendo tabla '{TABLE_NAME}'...")
    
    gdf.to_postgis(
        name=TABLE_NAME,
        con=engine,
        if_exists='replace', 
        index=False,
        chunksize=1000 
    )
    
    # 6. Post-Procesamiento SQL (CORREGIDO PARA SQLALCHEMY 2.0)
    print("   🔧 Optimizando índices SQL...")
    
    # Usamos 'begin()' para que haga el commit automático
    with engine.begin() as conn:
        # Envolvemos el string SQL en text()
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (h3_index);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST(geometry);"))
    
    print("   ✅ ¡ÉXITO! Tabla lista y optimizada.")

if __name__ == "__main__":
    load_data_to_postgis()
import pandas as pd
from sqlalchemy import create_engine, text
import h3
from shapely.geometry import Polygon
import geopandas as gpd
import os
import sys

# ==========================================
# 1. SETUP DE RUTAS
# ==========================================
# Ubicación actual: .../spatia-li/etl/02_load_to_postgis.py
current_dir = os.path.dirname(os.path.abspath(__file__))

# Subimos 1 nivel para llegar a la raíz (spatia-li)
project_root = os.path.dirname(current_dir) 
sys.path.append(project_root)

# Importamos config
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

# RUTA CORRECTA AL DATASET PROCESADO
CSV_PATH = os.path.join(project_root, "data", "processed", "final_dataset.csv")

# ==========================================
# 2. PROCESO DE CARGA
# ==========================================
def load_data_to_postgis():
    print("🚀 PASO 02: CARGA A POSTGIS (GEOMETRÍA)...")
    
    # 1. Leer el CSV Maestro
    print(f"   📥 Leyendo archivo: {CSV_PATH}...")
    
    if not os.path.exists(CSV_PATH):
        print(f"   ❌ ERROR CRÍTICO: No encuentro el archivo.")
        print(f"      Ruta buscada: {CSV_PATH}")
        print("      -> Asegúrate de haber ejecutado el Paso 01 correctamente.")
        return

    try:
        df = pd.read_csv(CSV_PATH)
        print(f"      -> Filas cargadas: {len(df)}")
    except Exception as e:
        print(f"   ❌ Error leyendo CSV: {e}")
        return

    if df.empty:
        print("   ⚠️ El CSV está vacío. Nada que cargar.")
        return

    # 2. Generar Geometría (Polígonos H3)
    print("   ⬡ Convirtiendo índices H3 a Polígonos Reales...")
    
    def get_hex_polygon(h3_index):
        try:
            boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
            # H3 devuelve coordenadas, Shapely crea el polígono
            return Polygon(boundary)
        except:
            return None

    df['geometry'] = df['h3_index'].apply(get_hex_polygon)
    
    # Eliminamos filas si falló la conversión de geometría
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
    
    try:
        gdf.to_postgis(
            name=TABLE_NAME,
            con=engine,
            if_exists='replace', # Borra la tabla vieja y crea una nueva limpia
            index=False,
            chunksize=1000 
        )
    except Exception as e:
        print(f"   ❌ Error subiendo a PostGIS: {e}")
        return
    
    # 6. Post-Procesamiento SQL (Índices y Primary Keys)
    print("   🔧 Optimizando índices SQL...")
    
    try:
        with engine.begin() as conn:
            # Añadir Primary Key para que sea rápido buscar por ID
            conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (h3_index);"))
            # Añadir Índice Espacial para que las búsquedas geográficas vuelen
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST(geometry);"))
            
        print("   ✅ ¡ÉXITO! Tabla lista y optimizada.")
        
    except Exception as e:
        print(f"   ⚠️ Aviso: La tabla se cargó, pero fallaron los índices: {e}")

if __name__ == "__main__":
    load_data_to_postgis()
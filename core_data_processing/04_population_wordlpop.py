import sys
import os
import gc
import re
import glob
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from rasterstats import zonal_stats

# 1. Configuración de rutas
DEMOGRAPHICS_DIR = "/home/jesus/spatia-li/data/raw/worldpop_parts"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL, ACTIVE_CITIES
except ImportError:
    print("❌ Error: No se encuentra 'conf.py'.")
    sys.exit(1)

SCHEMA = "core"
TABLE = "demographics"

def parse_filename_r2025(filename):
    """Analiza el nombre del archivo para extraer género y edad."""
    match = re.search(r'esp_([tmf])_(\d{2})_', filename.lower())
    if not match: return None
    
    gender_code = match.group(1)
    age_code = match.group(2)
    
    gender_map = {'t': 'total', 'm': 'male', 'f': 'female'}
    gender_label = gender_map.get(gender_code, 'total')

    if age_code == '00': age_label = "0_1y"
    elif age_code == '01': age_label = "1_4y"
    elif age_code == '90': age_label = "90plus"
    else:
        age_start = int(age_code)
        age_label = f"{age_start}_{age_start + 4}y"
        
    return f"pop_{gender_label}_{age_label}"

def ensure_column_exists(engine, column_name):
    """Asegura la tabla y la columna."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                h3_id TEXT PRIMARY KEY,
                city TEXT,
                year INTEGER
            );
        """))
        try:
            conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE} ADD COLUMN IF NOT EXISTS {column_name} FLOAT DEFAULT 0;"))
            conn.commit()
        except: pass

def calculate_total_population(engine, columns):
    """Suma todas las columnas de población en una nueva columna p_t."""
    if not columns: return
    
    print(f"\n🧮 Calculando columna P_T (Suma de {len(columns)} franjas)...")
    
    # Construimos la parte de la suma: col1 + col2 + col3...
    sum_expression = " + ".join([f"COALESCE({col}, 0)" for col in columns])
    
    with engine.connect() as conn:
        # 1. Crear columna p_t si no existe
        conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE} ADD COLUMN IF NOT EXISTS p_t FLOAT DEFAULT 0;"))
        
        # 2. Realizar el update masivo
        sql = f"UPDATE {SCHEMA}.{TABLE} SET p_t = {sum_expression};"
        conn.execute(text(sql))
        conn.commit()
    
    print("   ✅ Columna P_T actualizada correctamente.")

def process_single_raster(engine, file_path, column_name):
    """Proceso individual por raster y ciudad."""
    print(f"\n📊 Procesando: {column_name}")
    ensure_column_exists(engine, column_name)

    for city in ACTIVE_CITIES:
        print(f"   ↳ {city}...", end='\r')
        sql = text(f"SELECT h3_id, geometry FROM {SCHEMA}.hexagons WHERE city = :city")
        gdf_hex = gpd.read_postgis(sql, engine, params={"city": city}, geom_col="geometry")
        if gdf_hex.empty: continue

        stats = zonal_stats(vectors=gdf_hex['geometry'], raster=file_path, stats=['sum'])
        gdf_hex[column_name] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
        
        temp_table = f"temp_dm_{city.lower()}"
        gdf_hex[['h3_id', column_name]].to_sql(temp_table, engine, if_exists='replace', index=False)

        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.{TABLE} (h3_id, city, year)
                SELECT h3_id, '{city}', 2025 FROM {temp_table}
                ON CONFLICT (h3_id) DO NOTHING;
            """))
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{TABLE} AS target
                SET {column_name} = source.{column_name}
                FROM {temp_table} AS source
                WHERE target.h3_id = source.h3_id;
            """))
            conn.execute(text(f"DROP TABLE {temp_table};"))
            conn.commit()
        del gdf_hex
        gc.collect()

def main():
    engine = create_engine(DB_URL)
    raster_files = glob.glob(os.path.join(DEMOGRAPHICS_DIR, "*.tif"))
    
    if not raster_files:
        print(f"❌ No hay archivos en: {DEMOGRAPHICS_DIR}")
        return

    print(f"📂 Encontrados {len(raster_files)} archivos de población R2025A.")
    
    processed_columns = []

    # 1. Procesar cada archivo
    for file_path in sorted(raster_files):
        col_name = parse_filename_r2025(os.path.basename(file_path))
        if col_name:
            process_single_raster(engine, file_path, col_name)
            processed_columns.append(col_name)
        else:
            print(f"⚠️ Ignorado: {os.path.basename(file_path)}")

    # 2. NUEVO: Calcular el total después de procesar todo
    calculate_total_population(engine, processed_columns)

    print("\n🏁 PROCESO COMPLETADO. Datos e índice P_T generados.")

if __name__ == "__main__":
    main()
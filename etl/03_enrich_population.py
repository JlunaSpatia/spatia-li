

# 📍 RUTA DEL ARCHIVO LOCAL (Ya descargado por ti)
# Fuente original: https://hub.worldpop.org/geodata/summary?id=75487
# Archivo: Spain 2025 population projection (100m resolution)
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text  # <--- IMPORTAMOS text
from rasterstats import zonal_stats
import os

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
RASTER_PATH = "data/raw/esp_pop_2025_CN_100m_R2025A_v1.tif"

def enrich_with_population():
    print("👥 INICIANDO ENRIQUECIMIENTO DEMOGRÁFICO (WORLDPOP 2025)...")
    
    if not os.path.exists(RASTER_PATH):
        print(f"❌ ERROR: No encuentro el archivo en: {RASTER_PATH}")
        return

    # 1. LEER HEXÁGONOS
    print("   Leyendo hexágonos de PostGIS...")
    engine = create_engine(DB_URL)
    sql = "SELECT h3_index, city, geometry FROM retail_hexagons"
    gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    print(f"📊 Procesando {len(gdf)} hexágonos...")

    # 2. ZONAL STATISTICS
    try:
        stats = zonal_stats(vectors=gdf['geometry'], raster=RASTER_PATH, stats=['sum'])
    except Exception as e:
        print(f"❌ Error Raster: {e}")
        return

    # 3. LIMPIEZA
    gdf['pop_2025'] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    gdf['pop_2025'] = gdf['pop_2025'].round(0).astype(int)
    
    print(f"   Total Población: {gdf['pop_2025'].sum():,}")

    # 4. GUARDAR
    table_name = "retail_hexagons_enriched"
    print(f"💾 Guardando tabla '{table_name}'...")
    
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
    
    # 5. ÍNDICE (CORREGIDO)
    with engine.connect() as conn:
        # Envolvemos el string en text() para que SQLAlchemy no se queje
        conn.execute(text(f"CREATE INDEX idx_{table_name}_h3 ON {table_name}(h3_index);"))
        conn.commit() # Aseguramos el guardado
        
    print("✅ ¡PROCESO TERMINADO SIN ERRORES!")

if __name__ == "__main__":
    enrich_with_population()
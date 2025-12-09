import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from rasterstats import zonal_stats
import os

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
# El archivo que acabas de generar
RASTER_PATH = "data/raw/target_audience_combined.tif"

def enrich_with_target_pop():
    print("🎯 PASO 04: ENRIQUECIMIENTO POBLACIÓN TARGET (15-35 AÑOS)...")
    
    if not os.path.exists(RASTER_PATH):
        print(f"❌ ERROR: No encuentro {RASTER_PATH}")
        return

    engine = create_engine(DB_URL)
    
    # 1. LEER GEOMETRÍAS DE LA BASE DE DATOS
    # Leemos de la tabla enriched si existe (para mantener coherencia), si no de la raw
    print("   Leyendo zonas hexagonales...")
    try:
        sql = "SELECT h3_index, geometry FROM retail_hexagons_enriched"
        gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
    except:
        # Fallback si no existe la enriched aún
        sql = "SELECT h3_index, geometry FROM retail_hexagons"
        gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    print(f"   📊 Cruzando {len(gdf)} zonas con el Raster de Target...")

    # 2. ZONAL STATISTICS (CRUCE)
    # Sumamos los píxeles de gente joven dentro de cada hexágono
    stats = zonal_stats(
        vectors=gdf['geometry'], 
        raster=RASTER_PATH, 
        stats=['sum']
    )

    # 3. LIMPIEZA
    gdf['target_pop'] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    gdf['target_pop'] = gdf['target_pop'].round(0).astype(int)
    
    total_target = gdf['target_pop'].sum()
    print(f"   🔥 Total Target (Jóvenes) detectados: {total_target:,}")

    # 4. GUARDAR (UPDATE SQL SEGURO)
    print("💾 Actualizando Base de Datos...")
    
    # Subimos solo los datos nuevos a una tabla temporal
    gdf[['h3_index', 'target_pop']].to_sql('temp_target_pop', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # A. Aseguramos que la tabla destino existe (si vienes directo del paso 2)
        # Si no existe 'retail_hexagons_enriched', la creamos copiando la base
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS retail_hexagons_enriched AS 
            SELECT * FROM retail_hexagons;
        """))
        
        # B. Crear la columna si no existe
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS target_pop INTEGER;"))
        
        # C. Update Masivo
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET target_pop = s.target_pop
            FROM temp_target_pop AS s
            WHERE m.h3_index = s.h3_index;
        """))
        
        # D. Limpieza
        conn.execute(text("DROP TABLE temp_target_pop;"))
        conn.commit()

    print("✅ DATOS DE TARGET INTEGRADOS. Tu mapa ahora sabe dónde está la juventud.")
    
    # Check rápido
    top = gdf.sort_values('target_pop', ascending=False).head(1)
    print(f"   🏆 Zona más joven: {top.iloc[0]['h3_index']} con {top.iloc[0]['target_pop']} jóvenes.")

if __name__ == "__main__":
    enrich_with_target_pop()
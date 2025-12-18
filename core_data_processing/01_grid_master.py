import sys
import os

# 1. Truco para importar config.py desde la carpeta superior
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import h3
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, box, mapping
from sqlalchemy import create_engine
from config import DB_URL, CITY_BBOXES, ACTIVE_CITIES, H3_RESOLUTION

def generate_master_grid():
    print(f"🌍 Iniciando generación de Grid H3 (Res: {H3_RESOLUTION})...")
    
    # Determinar qué ciudades procesar
    cities_to_process = ACTIVE_CITIES if ACTIVE_CITIES else CITY_BBOXES.keys()
    all_hexagons = []
    
    for city in cities_to_process:
        if city not in CITY_BBOXES:
            print(f"⚠️ La ciudad {city} no tiene BBOX definido en config. Saltando.")
            continue
            
        print(f"   📍 Procesando: {city}")
        coords = CITY_BBOXES[city]
        
        # Crear polígono desde el BBOX del config
        city_polygon = box(coords['min_lon'], coords['min_lat'], coords['max_lon'], coords['max_lat'])
        
        # Llenar el polígono con hexágonos H3
        geo_json = mapping(city_polygon)
        hex_ids = h3.polyfill(geo_json, H3_RESOLUTION, geo_json_conformant=True)
        
        if not hex_ids:
            print(f"⚠️ No se generaron hexágonos para {city}. Revisa coordenadas.")
            continue

        # Crear DataFrame temporal
        df_city = pd.DataFrame(hex_ids, columns=['h3_id'])
        df_city['city'] = city # Guardamos a qué ciudad pertenece
        
        # Generar geometría (Polígonos) a partir del ID de H3
        df_city['geometry'] = df_city['h3_id'].apply(
            lambda x: Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
        )
        all_hexagons.append(df_city)

    if not all_hexagons:
        print("❌ No se generaron hexágonos en total. Revisa ACTIVE_CITIES.")
        return

    # Unificar todo en un solo GeoDataFrame
    final_df = pd.concat(all_hexagons, ignore_index=True)
    gdf_hex = gpd.GeoDataFrame(final_df, geometry='geometry', crs="EPSG:4326")
    
    # 🛠️ CORRECCIÓN CRÍTICA: 
    # Convertimos la columna 'h3_id' en el índice del DataFrame.
    # Así, al subirlo a SQL, PostGIS lo usará como Primary Key y no intentará
    # crear una columna duplicada.
    gdf_hex.set_index("h3_id", inplace=True)
    
    # --- SUBIDA A BBDD ---
    try:
        engine = create_engine(DB_URL)
        print(f"💾 Guardando {len(gdf_hex)} hexágonos en la tabla 'core.hexagons'...")
        
        gdf_hex.to_postgis(
            name="hexagons",
            con=engine,
            schema="core",
            if_exists="replace", # Borra tabla vieja, crea nueva
            index=True,          # Guarda el índice (que ahora es h3_id)
            index_label="h3_id"  # Nombre de la columna en Postgres
        )
        print("✅ Grid Maestro actualizado exitosamente en BBDD.")
        
    except Exception as e:
        print(f"❌ Error subiendo a base de datos: {e}")

if __name__ == "__main__":
    generate_master_grid()
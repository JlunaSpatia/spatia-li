import json
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely.geometry import Point
import config

# --- CONFIGURACIÓN DE BASE DE DATOS Y H3 ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia" 

def generate_postgis_grid(city_name):
    print(f"🧠 GENERANDO MALLA (INTERSECCIÓN EN MEMORIA) PARA: {city_name}")

    # --- 1. Conexión y Carga de Hexágonos (El Filtro) ---
    # Cargamos el polígono H3 enriquecido directamente de PostGIS a un GeoDataFrame (GDF)
    
    SQL_QUERY_HEX = f"""
    SELECT h3_index, target_pop, geometry 
    FROM public.retail_hexagons_enriched 
    WHERE city = 'Madrid'; -- Usamos 'Madrid' según tu verificación
    """
    print("1. Cargando hexágonos poblados de PostGIS a memoria (RAW data)...")
    
    try:
        engine = create_engine(DB_URL)
        # Forzamos la lectura de la geometría y la asignación del CRS a 4326 en Python
        gdf_hex_raw = gpd.read_postgis(SQL_QUERY_HEX, engine, geom_col='geometry', crs="EPSG:4326")
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: Fallo al cargar los hexágonos. ¿La tabla 'retail_hexagons_enriched' existe?")
        print(f"   Detalle del error (Revisa si el servicio PostGIS está UP): {e}")
        return

    # 2. Filtrar los hexágonos en memoria (target_pop > 0)
    gdf_hex_filtered = gdf_hex_raw[gdf_hex_raw['target_pop'] > 0].copy()
    
    print(f"   ✅ {len(gdf_hex_raw)} hexágonos cargados. {len(gdf_hex_filtered)} tienen población útil.")
    
    # --- 3. GENERAR LOS 700 PUNTOS BRUTOS EN PYTHON (Data a Intersecar) ---
    print("2. Generando malla matemática bruta (700 puntos) con CRS limpio...")
    bbox_conf = config.CITIES[city_name]
    step = config.GRID_STEP
    
    lat_steps = np.arange(bbox_conf["min_lat"], bbox_conf["max_lat"], step)
    lon_steps = np.arange(bbox_conf["min_lon"], bbox_conf["max_lon"], step)
    
    # Crear un GeoDataFrame de Puntos (700 celdas)
    points_list = [{'lat': float(lat), 'lon': float(lon)} for lat in lat_steps for lon in lon_steps]

    gdf_bruto = gpd.GeoDataFrame(
        points_list,
        geometry=gpd.points_from_xy([p['lon'] for p in points_list], [p['lat'] for p in points_list]),
        crs="EPSG:4326" # CRS CLAVE: Ambos deben ser 4326 para el join
    )

    # --- 4. EJECUTAR INTERSECCIÓN ESPACIAL EN MEMORIA (gpd.sjoin) ---
    print("3. Ejecutando Spatial Join (Intersección de Puntos con Hexágonos Poblados)...")
    
    try:
        # El resultado incluye solo los puntos que caen DENTRO de un hexágono con target_pop > 0.
        gdf_intersected = gpd.sjoin(
            gdf_bruto, 
            gdf_hex_filtered[['h3_index', 'geometry']], 
            how="inner", 
            predicate="within"
        )
    except Exception as e:
        print(f"❌ ERROR CRÍTICO en Spatial Join (gpd.sjoin). Falló la intersección.")
        print(f"   Detalle: {e}")
        return

    # 5. Quedarnos con los puntos únicos finales
    df_filtered_final = gdf_intersected[['lat', 'lon']].drop_duplicates()
    
    # --- 6. CONVERTIR A FORMATO SCRAPER Y GUARDAR CACHÉ FINAL ---
    smart_grid = []
    preview_features = []
    
    for _, row in df_filtered_final.iterrows():
        lat, lon = row['lat'], row['lon']
        coords_str = f"@{lat:.5f},{lon:.5f},{config.ZOOM_LEVEL}"
        smart_grid.append(coords_str)
        
        # Para visualización
        preview_features.append({
            "type": "Feature",
            "properties": {"status": "kept"},
            "geometry": {"type": "Point", "coordinates": [lon, lat]}
        })

    # --- 7. GUARDAR CACHÉ FINAL ---
    output_dir = "market_scanner/cache"
    os.makedirs(output_dir, exist_ok=True)
    
    grid_file = os.path.join(output_dir, f"{city_name}_SMART_GRID.json")
    with open(grid_file, "w") as f:
        json.dump(smart_grid, f)

    preview_file = f"PREVIEW_{city_name}_FINAL_PYTHON_JOIN.geojson"
    with open(preview_file, "w") as f:
        json.dump({"type": "FeatureCollection", "features": preview_features}, f)
            
    print("-" * 60)
    print("✅ PROCESO DE FILTRADO FINALIZADO (Temporal en Memoria).")
    print(f"   📉 Puntos Brutos Iniciales: {len(gdf_bruto)}")
    print(f"   🎯 Puntos ÚTILES Filtrados: {len(smart_grid)} (Este es el número que lanzará el script 01).")
    print(f"   🚀 Grid listo. Ejecuta 'python market_scanner/01_fetch_city.py'.")
    print("-" * 60)

if __name__ == "__main__":
    generate_postgis_grid("MADRID")
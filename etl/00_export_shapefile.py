import osmnx as ox
import pandas as pd
import geopandas as gpd
import h3
from shapely.geometry import Polygon
import warnings

# --- CONFIGURACIÓN ---
H3_RES = 9
OUTPUT_FOLDER = "data_exchange"  # Carpeta para intercambiar datos con tu tool
CITIES = ["Madrid, Spain", "Valencia, Spain"]

warnings.filterwarnings("ignore")

def get_hexagons_as_gdf(city_name):
    print(f"📥 Procesando geometría de {city_name}...")
    gdf_city = ox.geocode_to_gdf(city_name)
    gdf_exploded = gdf_city.explode(index_parts=False)
    all_hexagons = set()
    
    # 1. Generar índices H3
    for _, row in gdf_exploded.iterrows():
        try:
            if row.geometry.geom_type == 'Polygon':
                hexs = h3.polyfill(row.geometry.__geo_interface__, H3_RES, geo_json_conformant=True)
                all_hexagons.update(hexs)
        except: continue
    
    # 2. Convertir índices a Geometrías (Polígonos) para el Shapefile
    print(f"⬡ Convirtiendo {len(all_hexagons)} hexágonos a polígonos...")
    
    hex_data = []
    for h_id in all_hexagons:
        # Convertir H3 a Polígono
        poly = Polygon(h3.h3_to_geo_boundary(h_id, geo_json=True))
        hex_data.append({'h3_index': h_id, 'city': city_name.split(",")[0], 'geometry': poly})
    
    gdf = gpd.GeoDataFrame(hex_data)
    gdf.set_crs(epsg=4326, inplace=True) # WGS84
    return gdf

# --- EJECUCIÓN ---
print("🚀 GENERADOR DE INPUT PARA TOOL DE POBLACIÓN")
gdfs = []
for city in CITIES:
    gdfs.append(get_hexagons_as_gdf(city))

full_gdf = pd.concat(gdfs)

# Crear carpeta si no existe
import os
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Guardar Shapefile
output_path = f"{OUTPUT_FOLDER}/hexagons_input.shp"
print(f"💾 Guardando Shapefile en: {output_path}")

# Nota: Los Shapefiles cortan los nombres de columnas a 10 caracteres. 
# 'h3_index' tiene 8 chars, así que perfecto.
full_gdf.to_file(output_path, driver='ESRI Shapefile')

print("✅ ¡Listo! Ahora alimenta tu herramienta con este archivo.")
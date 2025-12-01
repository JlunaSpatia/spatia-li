import osmnx as ox
import geopandas as gpd
import pandas as pd
import h3
import requests
from shapely.geometry import Point
import warnings
import sys
import os

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import TARGET_CITIES, H3_RESOLUTION, OSRM_WALK_URL

warnings.filterwarnings("ignore")
CSV_PATH = "etl/final_dataset.csv"

print(f"🚀 PASO 01: GESTIÓN DE CIUDADES (INCREMENTAL)...")

# --- 1. LÓGICA DE DELTA (LO NUEVO VS LO VIEJO) ---
cities_to_process = []
df_existing = pd.DataFrame()

if os.path.exists(CSV_PATH):
    print("   📂 Encontrado dataset previo. Analizando...")
    df_existing = pd.read_csv(CSV_PATH)
    
    # Ciudades que ya tenemos
    existing_cities = df_existing['city'].unique().tolist()
    print(f"      -> Ciudades ya procesadas: {existing_cities}")
    
    # Identificar cuáles faltan
    # Limpiamos nombre del config ("Sevilla, Spain" -> "Sevilla") para comparar
    for target in TARGET_CITIES:
        clean_target_name = target.split(",")[0]
        if clean_target_name not in existing_cities:
            cities_to_process.append(target)
        else:
            print(f"      ⏩ Saltando {target} (Ya existe).")
else:
    print("   🆕 No hay datos previos. Se procesará todo.")
    cities_to_process = TARGET_CITIES

if not cities_to_process:
    print("   ✅ Todas las ciudades del config ya están procesadas. Nada que hacer.")
    # Importante: Si no hay nada nuevo, salimos para no romper nada
    exit()

print(f"   🔨 Ciudades a procesar hoy: {cities_to_process}")

# --- 2. FUNCIONES (Mismas de siempre) ---
def get_hexagons_from_city(city_query):
    print(f"   📥 Descargando límites de: {city_query}...")
    try:
        gdf_city = ox.geocode_to_gdf(city_query)
    except: return pd.DataFrame()

    gdf_exploded = gdf_city.explode(index_parts=False)
    all_hexagons = set()
    
    for _, row in gdf_exploded.iterrows():
        try:
            if row.geometry.geom_type == 'Polygon':
                hexs = h3.polyfill(row.geometry.__geo_interface__, H3_RESOLUTION, geo_json_conformant=True)
                all_hexagons.update(hexs)
        except: continue
    
    clean_name = city_query.split(",")[0]
    df_hex = pd.DataFrame(list(all_hexagons), columns=['h3_index'])
    df_hex['city'] = clean_name
    df_hex['lat'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex['lon'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[1])
    return df_hex

def get_osrm_dist(origin_lat, origin_lon, dest_gdf):
    if dest_gdf.empty: return 5000
    origin_point = Point(origin_lon, origin_lat)
    dest_gdf = dest_gdf.copy()
    dest_gdf['temp_dist'] = dest_gdf.distance(origin_point)
    nearest = dest_gdf.sort_values('temp_dist').iloc[0]
    
    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest.geometry.x},{nearest.geometry.y}?overview=false"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res and len(res['routes']) > 0:
                return res['routes'][0]['duration']
    except: pass
    return 5000

# --- 3. PROCESAMIENTO (SOLO LO NUEVO) ---
new_city_dfs = []
city_areas = [] 

for city in cities_to_process:
    df = get_hexagons_from_city(city)
    if not df.empty:
        new_city_dfs.append(df)
        city_areas.append(ox.geocode_to_gdf(city).geometry[0])

if not new_city_dfs:
    print("❌ Error generando hexágonos.")
    exit()

# Unimos solo lo nuevo temporalmente para calcular OSRM
df_new_batch = pd.concat(new_city_dfs).reset_index(drop=True)

# Descargar POIs (Solo para las zonas nuevas)
print("   🏗️ Descargando POIs para las NUEVAS zonas...")
tags = {
    "cafe": {"amenity": ["cafe", "pub"]},
    "gym": {"leisure": ["fitness_centre", "sports_centre"]},
    "shop": {"shop": ["clothes", "mall"]},
    "transit": {"highway": "bus_stop", "railway": "subway_entrance"}
}
pois_data = {}
for key, tag in tags.items():
    gdfs = []
    for area in city_areas:
        try:
            gdf = ox.features_from_polygon(area, tags=tag)
            gdf['geometry'] = gdf.geometry.centroid
            gdfs.append(gdf)
        except: pass
    pois_data[key] = pd.concat(gdfs) if gdfs else gpd.GeoDataFrame()

# Cálculo OSRM
print(f"   🚀 Calculando OSRM para {len(df_new_batch)} hexágonos nuevos...")
for idx, row in df_new_batch.iterrows():
    if idx % 100 == 0: print(f"      Procesando {idx}...", end="\r")
    df_new_batch.at[idx, 'dist_cafe'] = get_osrm_dist(row['lat'], row['lon'], pois_data.get('cafe', gpd.GeoDataFrame()))
    df_new_batch.at[idx, 'dist_gym'] = get_osrm_dist(row['lat'], row['lon'], pois_data.get('gym', gpd.GeoDataFrame()))
    df_new_batch.at[idx, 'dist_shop'] = get_osrm_dist(row['lat'], row['lon'], pois_data.get('shop', gpd.GeoDataFrame()))
    df_new_batch.at[idx, 'dist_transit'] = get_osrm_dist(row['lat'], row['lon'], pois_data.get('transit', gpd.GeoDataFrame()))

# --- 4. FUSIÓN Y GUARDADO ---
print("\n   💾 Fusionando histórico + nuevos...")

if not df_existing.empty:
    # Concatenamos lo viejo con lo nuevo
    df_final = pd.concat([df_existing, df_new_batch], ignore_index=True)
else:
    df_final = df_new_batch

# Guardamos el acumulado total
df_final.to_csv(CSV_PATH, index=False)
print(f"✅ DATASET ACTUALIZADO. Total ciudades: {df_final['city'].unique()}")
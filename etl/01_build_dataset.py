import osmnx as ox
import geopandas as gpd
import pandas as pd
import h3
import requests
from shapely.geometry import Point
import warnings
import time

# --- CONFIGURACIÓN ---
OSRM_WALK_URL = "http://localhost:5001"
H3_RES = 9 
warnings.filterwarnings("ignore")

print("🚀 INICIANDO ETL DE PRODUCCIÓN - RETAIL GENOME")

# 1. TIENDAS BLUE BANANA (Label de Entrenamiento)
bb_stores = pd.DataFrame([
    {"city": "Madrid", "name": "BB Fuencarral", "lat": 40.4287, "lon": -3.7020},
    {"city": "Madrid", "name": "BB Goya", "lat": 40.4256, "lon": -3.6808},
    {"city": "Valencia", "name": "BB Valencia", "lat": 39.4735, "lon": -0.3725},
])

# 2. FUNCIONES BASE
def get_hexagons_from_city(city_name):
    print(f"📥 Descargando límites de {city_name}...")
    gdf_city = ox.geocode_to_gdf(city_name)
    gdf_exploded = gdf_city.explode(index_parts=False)
    all_hexagons = set()
    
    for _, row in gdf_exploded.iterrows():
        try:
            geom = row.geometry
            if geom.geom_type == 'Polygon':
                hexs = h3.polyfill(geom.__geo_interface__, H3_RES, geo_json_conformant=True)
                all_hexagons.update(hexs)
        except: continue
    
    df_hex = pd.DataFrame(list(all_hexagons), columns=['h3_index'])
    df_hex['city'] = city_name.split(",")[0]
    df_hex['lat'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex['lon'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[1])
    return df_hex

def get_osrm_dist(origin_lat, origin_lon, dest_gdf):
    if dest_gdf.empty: return 5000 # Penalización default (50 min)
    
    # Pre-filtro euclidiano para velocidad
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

# 3. GENERAR REJILLA COMPLETA
df_mad = get_hexagons_from_city("Madrid, Spain")
df_val = get_hexagons_from_city("Valencia, Spain")
df_final = pd.concat([df_mad, df_val]).reset_index(drop=True)
print(f"✅ Total Hexágonos a procesar: {len(df_final)}")

# 4. BAJAR TODOS LOS POIS (Contexto)
tags = {
    "cafe": {"amenity": ["cafe", "pub"]},
    "gym": {"leisure": ["fitness_centre", "sports_centre"]},
    "shop": {"shop": ["clothes", "mall"]},
    "transit": {"highway": "bus_stop", "railway": "subway_entrance"}
}

pois_data = {}

print("🏗️ Descargando Infraestructura (POIs)...")
areas = [ox.geocode_to_gdf("Madrid, Spain").geometry[0], ox.geocode_to_gdf("Valencia, Spain").geometry[0]]

for key, tag in tags.items():
    print(f"   - Descargando {key}...")
    gdfs = []
    for area in areas:
        try:
            gdf = ox.features_from_polygon(area, tags=tag)
            gdfs.append(gdf)
        except: pass
    
    merged = pd.concat(gdfs)
    # Normalizar a Centroides
    merged['geometry'] = merged.geometry.centroid
    pois_data[key] = merged
    print(f"     > {len(merged)} {key}s encontrados.")

# 5. ENRIQUECIMIENTO MASIVO (Loop Principal)
print("🚀 Calculando métricas OSRM para TODOS los hexágonos (Paciencia, tardará unos min)...")

# Contadores para feedback
start_time = time.time()

# Convertimos a listas para iterar rápido
results = []
total = len(df_final)

for idx, row in df_final.iterrows():
    if idx % 100 == 0: print(f"   Procesando {idx}/{total}...", end="\r")
    
    # Calcular distancias
    dist_cafe = get_osrm_dist(row['lat'], row['lon'], pois_data['cafe'])
    dist_gym = get_osrm_dist(row['lat'], row['lon'], pois_data['gym'])
    dist_shop = get_osrm_dist(row['lat'], row['lon'], pois_data['shop'])
    dist_transit = get_osrm_dist(row['lat'], row['lon'], pois_data['transit'])
    
    # Etiquetar si es una tienda Blue Banana (Target)
    # Un hexágono es "éxito" si una tienda cae dentro de él
    is_bb = 0
    bb_point = Point(row['lon'], row['lat'])
    # H3 tiene una función geo_to_h3, verificamos si las tiendas caen en este índice
    for _, store in bb_stores.iterrows():
        store_h3 = h3.geo_to_h3(store['lat'], store['lon'], H3_RES)
        if store_h3 == row['h3_index']:
            is_bb = 1
            print(f"   🎯 ¡BOOM! Tienda encontrada en {row['h3_index']} ({store['city']})")
    
    results.append({
        "h3_index": row['h3_index'],
        "city": row['city'],
        "lat": row['lat'],
        "lon": row['lon'],
        "dist_cafe": dist_cafe,
        "dist_gym": dist_gym,
        "dist_shop": dist_shop,
        "dist_transit": dist_transit,
        "is_blue_banana": is_bb
    })

# 6. GUARDAR
final_df = pd.DataFrame(results)
final_df.to_csv("etl/final_dataset.csv", index=False)
print(f"\n💾 Dataset guardado: etl/final_dataset.csv ({len(final_df)} filas)")
print(f"⏱️ Tiempo total: {(time.time() - start_time)/60:.1f} minutos")
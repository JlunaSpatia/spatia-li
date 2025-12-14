import osmnx as ox
import geopandas as gpd
import pandas as pd
import h3
import requests
from shapely.geometry import Polygon, Point, box
from sqlalchemy import create_engine
from rasterstats import zonal_stats
import warnings
import sys
import os

# ==========================================
# 1. SETUP DE RUTAS Y CONFIG
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) 
sys.path.append(project_root)

try:
    from config import (
        DB_CONNECTION_STR, 
        DATA_DIR, 
        CITY_BBOXES,       # <--- Usamos tus nuevas coordenadas
        H3_RESOLUTION, 
        OSRM_WALK_URL,
        ACTIVE_CITIES      # <--- Tu filtro manual
    )
except ImportError:
    print("❌ Error: No encuentro 'config.py'.")
    sys.exit(1)

warnings.filterwarnings("ignore")

CSV_PATH = os.path.join(project_root, "data", "processed", "final_dataset.csv")
GHS_FILENAME = "GHS_BUILT_S_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"
GHS_PATH = os.path.join(project_root, DATA_DIR, GHS_FILENAME)

# ⚡ CONFIGURACIÓN DE VELOCIDAD (Modo Ferrari) ⚡
ox.settings.log_console = False        # Silencio para más velocidad
ox.settings.use_cache = True           # Usar disco
ox.settings.timeout = 1800             # Sin prisas si es grande
# Usamos el servidor rápido para evitar bloqueos en descargas grandes
ox.settings.overpass_endpoint = "https://overpass.kumi.systems/api/interpreter"

# ==========================================
# 2. FUNCIONES
# ==========================================

def filter_by_urban_footprint(df_hex):
    """Filtra hexágonos rurales usando Raster GHS."""
    if not os.path.exists(GHS_PATH):
        print(f"      ⚠️ No encuentro raster GHS. Saltando filtro.")
        return df_hex

    print(f"      🏗️ Filtrando zonas no urbanas (Raster GHS)...")
    
    hex_polygons = []
    for h in df_hex['h3_index']:
        geo_json = h3.h3_to_geo_boundary(h, geo_json=True)
        poly = Polygon(geo_json)
        hex_polygons.append(poly)
    
    gdf_temp = gpd.GeoDataFrame({'h3_index': df_hex['h3_index']}, geometry=hex_polygons, crs="EPSG:4326")
    
    stats = zonal_stats(
        vectors=gdf_temp['geometry'],
        raster=GHS_PATH,
        stats=['sum']
    )
    
    df_hex['built_up_score'] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    
    initial = len(df_hex)
    df_filtered = df_hex[df_hex['built_up_score'] > 50].copy()
    
    print(f"      ✂️ Eliminados {initial - len(df_filtered)} hexágonos rurales.")
    return df_filtered

def get_hexagons_from_bbox(city_name, bbox_dict):
    """Crea hexágonos a partir de tus coordenadas exactas."""
    # Creamos el cuadrado exacto
    bbox_poly = box(
        bbox_dict['min_lon'], 
        bbox_dict['min_lat'], 
        bbox_dict['max_lon'], 
        bbox_dict['max_lat']
    )

    try:
        hexs = h3.polyfill(bbox_poly.__geo_interface__, H3_RESOLUTION, geo_json_conformant=True)
    except: return pd.DataFrame()
    
    df_hex = pd.DataFrame(list(hexs), columns=['h3_index'])
    df_hex['city'] = city_name
    df_hex['lat'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex['lon'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[1])
    
    return df_hex

def get_osrm_dist(origin_lat, origin_lon, dest_gdf):
    if dest_gdf.empty: return 5000
    origin_point = Point(origin_lon, origin_lat)
    
    # Pre-filtro matemático (Vectorizado es instantáneo)
    dest_gdf_calc = dest_gdf.copy()
    dest_gdf_calc['temp_dist'] = dest_gdf_calc.distance(origin_point)
    nearest = dest_gdf_calc.sort_values('temp_dist').iloc[0]
    
    if nearest['temp_dist'] > 0.05: return 5000 

    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest.geometry.x},{nearest.geometry.y}?overview=false"
    try:
        # Timeout ultra-corto (0.1s) porque es local y debe volar
        r = requests.get(url, timeout=0.1) 
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res: return res['routes'][0]['duration']
    except: pass
    return 5000

def get_google_pois_from_db(city_name):
    print(f"      🛢️ Consultando PostGIS para {city_name}...")
    category_map = {
        'Cafetería': 'cafe', 'Bar': 'cafe', 'Panadería': 'cafe', 'Restaurante': 'cafe',
        'Gimnasio': 'gym', 'Tienda de ropa': 'shop', 'Centro comercial': 'shop', 'Tienda de deportes': 'shop'
    }
    categories_sql = "', '".join(category_map.keys())
    
    query = f"""
    SELECT latitude, longitude, search_category
    FROM public.retail_poi_master
    WHERE UPPER(city) LIKE '%%{city_name.upper()}%%' 
      AND search_category IN ('{categories_sql}')
    """
    
    engine = create_engine(DB_CONNECTION_STR)
    try:
        df_db = pd.read_sql(query, engine)
        if df_db.empty: return {}
        geometry = [Point(xy) for xy in zip(df_db.longitude, df_db.latitude)]
        gdf_google = gpd.GeoDataFrame(df_db, geometry=geometry, crs="EPSG:4326")
        
        pois_dict = {}
        df_db['internal_type'] = df_db['search_category'].map(category_map)
        for p_type in df_db['internal_type'].unique():
            pois_dict[p_type] = gdf_google[df_db['internal_type'] == p_type]
        return pois_dict
    except: return {}

# ==========================================
# 3. PROCESO PRINCIPAL
# ==========================================

print(f"🚀 GENERANDO DATASET (Modo Rápido)...")
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# 1. Selección de Ciudades
if ACTIVE_CITIES:
    cities_to_process = [c for c in ACTIVE_CITIES if c in CITY_BBOXES]
else:
    cities_to_process = list(CITY_BBOXES.keys())

for city_name in cities_to_process:
    print(f"\n🏙️  {city_name}")
    bbox = CITY_BBOXES[city_name]
    
    # A. Hexágonos
    df_hex = get_hexagons_from_bbox(city_name, bbox)
    if df_hex.empty: continue
    print(f"      ⬡ Brutos: {len(df_hex)}")

    # B. Filtro GHS
    df_hex = filter_by_urban_footprint(df_hex)
    if df_hex.empty: continue

    # C. Transporte (OSM) - Todo de golpe
    try:
        print("      🚌 Descargando transporte (OSM)...")
        # Usamos tupla bbox=(N, S, E, W) para OSMnx moderno
        gdf_transit = ox.features_from_bbox(
            bbox=(bbox['max_lat'], bbox['min_lat'], bbox['max_lon'], bbox['min_lon']),
            tags={"highway": "bus_stop", "railway": "subway_entrance"}
        )
        if not gdf_transit.empty:
            gdf_transit['geometry'] = gdf_transit.geometry.centroid
            print(f"      ✅ {len(gdf_transit)} paradas descargadas.")
    except Exception as e:
        print(f"      ⚠️ Error OSMnx: {e}")
        gdf_transit = gpd.GeoDataFrame()
        
    # D. POIs & Distancias
    google_pois = get_google_pois_from_db(city_name)
    total = len(df_hex)
    print(f"      🚀 Calculando rutas para {total} hexágonos...")
    
    # Init columnas
    for c in ['dist_cafe', 'dist_gym', 'dist_shop', 'dist_transit']: df_hex[c] = 5000

    # Bucle Rápido
    for idx, row in df_hex.iterrows():
        if idx % 500 == 0: print(f"         {idx}/{total}...", end="\r")
        
        if 'cafe' in google_pois: df_hex.at[idx, 'dist_cafe'] = get_osrm_dist(row['lat'], row['lon'], google_pois['cafe'])
        if 'gym' in google_pois: df_hex.at[idx, 'dist_gym'] = get_osrm_dist(row['lat'], row['lon'], google_pois['gym'])
        if 'shop' in google_pois: df_hex.at[idx, 'dist_shop'] = get_osrm_dist(row['lat'], row['lon'], google_pois['shop'])
        if not gdf_transit.empty: df_hex.at[idx, 'dist_transit'] = get_osrm_dist(row['lat'], row['lon'], gdf_transit)

    # Guardado Append (seguro)
    if os.path.exists(CSV_PATH):
        df_old = pd.read_csv(CSV_PATH)
        df_final = pd.concat([df_old[df_old['city'] != city_name], df_hex], ignore_index=True)
    else:
        df_final = df_hex

    df_final.to_csv(CSV_PATH, index=False)
    print(f"\n      ✅ Guardado.")
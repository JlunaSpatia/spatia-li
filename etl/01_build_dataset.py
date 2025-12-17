import pandas as pd
import geopandas as gpd
import h3
import requests
from shapely.geometry import Polygon, Point, box
from sqlalchemy import create_engine
from rasterstats import zonal_stats
import warnings
import sys
import os
import math

# ==========================================
# 1. SETUP Y CONFIGURACIÓN
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) 
sys.path.append(project_root)

try:
    from config import (
        DB_CONNECTION_STR, 
        DATA_DIR, 
        CITY_BBOXES, 
        H3_RESOLUTION, 
        OSRM_WALK_URL, 
        ACTIVE_CITIES
    )
except ImportError:
    print("❌ Error: No encuentro 'config.py'.")
    sys.exit(1)

warnings.filterwarnings("ignore")

CSV_PATH = os.path.join(project_root, "data", "processed", "final_dataset.csv")
GHS_FILENAME = "GHS_BUILT_S_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"
GHS_PATH = os.path.join(project_root, DATA_DIR, GHS_FILENAME)

print(f"🔧 Configuración cargada.")
print(f"📡 OSRM URL: {OSRM_WALK_URL} (Prioridad Máxima)")
print(f"🛢️ Transporte: Usando PostGIS Local")

# ==========================================
# 2. FUNCIONES DE CARGA
# ==========================================

def get_transport_from_db(bbox_dict):
    min_lon, min_lat = bbox_dict['min_lon'], bbox_dict['min_lat']
    max_lon, max_lat = bbox_dict['max_lon'], bbox_dict['max_lat']
    
    query = f"""
    SELECT * FROM osm_transport_points
    WHERE geometry && ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326)
    """
    engine = create_engine(DB_CONNECTION_STR)
    try:
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        return gdf
    except Exception as e:
        return gpd.GeoDataFrame()

def filter_by_urban_footprint(df_hex):
    if not os.path.exists(GHS_PATH): return df_hex
    
    hex_polygons = []
    for h in df_hex['h3_index']:
        geo_json = h3.h3_to_geo_boundary(h, geo_json=True)
        hex_polygons.append(Polygon(geo_json))
    
    gdf_temp = gpd.GeoDataFrame({'h3_index': df_hex['h3_index']}, geometry=hex_polygons, crs="EPSG:4326")
    stats = zonal_stats(vectors=gdf_temp['geometry'], raster=GHS_PATH, stats=['sum'])
    df_hex['built_up_score'] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    
    return df_hex[df_hex['built_up_score'] > 50].copy()

def get_hexagons_from_bbox(city_name, bbox_dict):
    bbox_poly = box(bbox_dict['min_lon'], bbox_dict['min_lat'], bbox_dict['max_lon'], bbox_dict['max_lat'])
    try:
        hexs = h3.polyfill(bbox_poly.__geo_interface__, H3_RESOLUTION, geo_json_conformant=True)
    except: return pd.DataFrame()
    
    df_hex = pd.DataFrame(list(hexs), columns=['h3_index'])
    df_hex['city'] = city_name
    df_hex['lat'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex['lon'] = df_hex['h3_index'].apply(lambda x: h3.h3_to_geo(x)[1])
    return df_hex

def get_google_pois_from_db(city_name):
    print(f"      🛢️ Consultando PostGIS (POIs Comerciales)...")
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
            if pd.notna(p_type):
                pois_dict[p_type] = gdf_google[df_db['internal_type'] == p_type]
        return pois_dict
    except: return {}

# ==========================================
# 3. LÓGICA DE CÁLCULO INTELIGENTE (PRIORIDAD OSRM)
# ==========================================

def calculate_distance_smart(row, dest_gdf):
    """
    1. Intenta OSRM SIEMPRE.
    2. Si falla OSRM, usa Euclidian.
    """
    # 0. ¿VACÍO?
    if dest_gdf is None or dest_gdf.empty: 
        return 5000, 'MAX'

    origin_lat, origin_lon = row['lat'], row['lon']
    origin_point = Point(origin_lon, origin_lat)
    
    # Buscamos el POI más cercano (necesario para saber a dónde rutear)
    series_dist = dest_gdf.distance(origin_point)
    nearest_idx = series_dist.idxmin()
    nearest_dist_deg = series_dist.loc[nearest_idx]
    nearest_poi = dest_gdf.loc[nearest_idx]

    # 1. FILTRO DE LEJANÍA OBVIA (> 5km)
    if nearest_dist_deg > 0.05: 
        return 5000, 'MAX'

    # 2. INTENTO OSRM (PRIORIDAD TOTAL)
    # No importa si está dentro o fuera, lo intentamos.
    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest_poi.geometry.x},{nearest_poi.geometry.y}?overview=false"
    try:
        r = requests.get(url, timeout=0.15) 
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res and len(res['routes']) > 0:
                duration = res['routes'][0]['duration']
                return duration, 'OSRM'
    except:
        pass # Fallo técnico o de snapping -> Pasamos a Plan B

    # 3. RESCATE EUCLIDIANO (Solo si OSRM falla)
    dy_meters = (nearest_poi.geometry.y - origin_lat) * 111132
    dx_meters = (nearest_poi.geometry.x - origin_lon) * 85000
    dist_meters = math.sqrt(dx_meters**2 + dy_meters**2)
    
    # Penalización x1.35
    walking_seconds = (dist_meters * 1.35) / 1.25
    
    return walking_seconds, 'EUCLID'

# ==========================================
# 4. PROCESO PRINCIPAL
# ==========================================

print(f"🚀 GENERANDO DATASET (PRIORIDAD OSRM + FALLBACK)...")
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

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

    # C. Transporte
    print("      🚌 Consultando transporte...")
    gdf_transit = get_transport_from_db(bbox)
    
    # D. POIs
    google_pois = get_google_pois_from_db(city_name)
    
    # E. Inicialización de columnas
    metrics = ['cafe', 'gym', 'shop', 'transit']
    for m in metrics:
        df_hex[f'dist_{m}'] = 5000.0
        df_hex[f'source_{m}'] = 'INIT' 

    total = len(df_hex)
    print(f"      🚀 Calculando tiempos (OSRM mandatorio)...")

    # F. Bucle de Cálculo
    for idx, row in df_hex.iterrows():
        if idx % 500 == 0: print(f"         {idx}/{total}...", end="\r")
        
        # Iteramos por métrica
        for metric, gdf_source in [
            ('cafe', google_pois.get('cafe')),
            ('gym', google_pois.get('gym')),
            ('shop', google_pois.get('shop')),
            ('transit', gdf_transit)
        ]:
            if gdf_source is not None and not gdf_source.empty:
                # Ya no pasamos mapas de presencia, solo la fila y el destino
                val, src = calculate_distance_smart(row, gdf_source)
                
                df_hex.at[idx, f'dist_{metric}'] = val
                df_hex.at[idx, f'source_{metric}'] = src

    # G. Guardado
    if os.path.exists(CSV_PATH):
        df_old = pd.read_csv(CSV_PATH)
        df_final = pd.concat([df_old[df_old['city'] != city_name], df_hex], ignore_index=True)
    else:
        df_final = df_hex

    df_final.to_csv(CSV_PATH, index=False)
    print(f"\n      ✅ Guardado datos de {city_name}.")

print("\n🏁 PROCESO TERMINADO.")
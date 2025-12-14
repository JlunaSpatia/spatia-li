import pandas as pd
import geopandas as gpd
import osmnx as ox
import requests
from shapely.geometry import Point
from sqlalchemy import create_engine
import sys
import os
import warnings

# ================= SETUP =================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Importamos variables del config
from config import DB_CONNECTION_STR, CITY_BBOXES, OSRM_WALK_URL, ACTIVE_CITIES

# Rutas
INPUT_GRID_PATH = os.path.join(project_root, "data", "processed", "01_urban_grid.csv")
OUTPUT_FINAL_PATH = os.path.join(project_root, "data", "processed", "02_final_dataset.csv")

warnings.filterwarnings("ignore")

# ================= FUNCIONES =================

def get_osrm_dist(origin_lat, origin_lon, dest_gdf):
    """Calcula distancia a pie (en segundos) usando servidor OSRM local."""
    if dest_gdf.empty: return 5000 # Penalización si no hay destino
    
    # 1. Filtro Euclidiano Rápido (aprox) para evitar llamadas inútiles a la API
    origin_point = Point(origin_lon, origin_lat)
    dest_gdf_calc = dest_gdf.copy()
    dest_gdf_calc['temp_dist'] = dest_gdf_calc.distance(origin_point)
    nearest = dest_gdf_calc.sort_values('temp_dist').iloc[0]
    
    # Si el punto más cercano está a >0.05 grados (~5km), devolvemos "lejos" directamente
    if nearest['temp_dist'] > 0.05: return 5000 

    # 2. Petición a OSRM
    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest.geometry.x},{nearest.geometry.y}?overview=false"
    try:
        r = requests.get(url, timeout=0.5)
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res and len(res['routes']) > 0:
                return res['routes'][0]['duration'] # Retorna segundos
    except: pass
    
    return 5000

def get_google_pois_from_db(city_name):
    """Extrae POIs de PostGIS filtrando por nombre de ciudad."""
    print(f"      🛢️ Consultando PostGIS para POIs en {city_name}...")
    
    category_map = {
        'Cafetería': 'cafe', 'Bar': 'cafe', 'Panadería': 'cafe', 'Restaurante': 'cafe',
        'Gimnasio': 'gym',
        'Tienda de ropa': 'shop', 'Centro comercial': 'shop', 'Tienda de deportes': 'shop'
    }
    categories_sql = "', '".join(category_map.keys())
    
    # Búsqueda parcial (LIKE) para que coincida "MADRID" con "Madrid, Spain"
    query = f"""
    SELECT latitude, longitude, search_category
    FROM public.retail_poi_master
    WHERE UPPER(city) LIKE '%%{city_name.upper()}%%' 
      AND search_category IN ('{categories_sql}')
    """
    
    engine = create_engine(DB_CONNECTION_STR)
    try:
        df_db = pd.read_sql(query, engine)
        if df_db.empty: 
            print("      ⚠️ No se encontraron POIs en la base de datos.")
            return {}
            
        geometry = [Point(xy) for xy in zip(df_db.longitude, df_db.latitude)]
        gdf_google = gpd.GeoDataFrame(df_db, geometry=geometry, crs="EPSG:4326")
        
        pois_dict = {}
        df_db['internal_type'] = df_db['search_category'].map(category_map)
        
        # Separamos en diccionarios para acceso rápido
        for p_type in df_db['internal_type'].unique():
            pois_dict[p_type] = gdf_google[df_db['internal_type'] == p_type]
            
        return pois_dict
    except Exception as e:
        print(f"      ❌ Error DB: {e}")
        return {}

def get_transit_osm(bbox):
    """Descarga paradas de bus/metro dentro del Bounding Box (Compatible OSMnx 2.0)."""
    try:
        tags = {"highway": "bus_stop", "railway": "subway_entrance"}
        
        # CORRECCIÓN AQUÍ: Usamos el argumento 'bbox' como tupla (N, S, E, W)
        gdf = ox.features_from_bbox(
            bbox=(bbox['max_lat'], bbox['min_lat'], bbox['max_lon'], bbox['min_lon']),
            tags=tags
        )
        
        if not gdf.empty:
            # Usamos el centroide porque las paradas a veces son polígonos
            gdf['geometry'] = gdf.geometry.centroid
            return gdf
            
    except Exception as e: 
        print(f"      ⚠️ Error OSMnx: {e}")
        
    return gpd.GeoDataFrame()

# ================= MAIN =================

print("🚀 INICIO: Cálculo de Distancias (Paso 2)")

if not os.path.exists(INPUT_GRID_PATH):
    print("❌ Error: No existe '01_urban_grid.csv'. Ejecuta el paso 1 primero.")
    sys.exit(1)

# 1. Cargar Malla
df_grid = pd.read_csv(INPUT_GRID_PATH)

# 2. Filtrar ciudades según Config (ACTIVE_CITIES)
if ACTIVE_CITIES:
    cities_to_process = [c for c in ACTIVE_CITIES if c in df_grid['city'].unique()]
    print(f"🎯 MODO FILTRO: Calculando solo para {cities_to_process}")
else:
    cities_to_process = df_grid['city'].unique()

dfs_result = []

for city in cities_to_process:
    print(f"\n🏙️  Enriqueciendo: {city}")
    
    # Filtramos hexágonos solo de esta ciudad
    df_city = df_grid[df_grid['city'] == city].copy()
    
    # Obtenemos BBox del config
    bbox = CITY_BBOXES.get(city)
    
    if not bbox:
        print(f"⚠️ No hay BBox en config para {city}. Saltando.")
        continue

    # A. Descargar Transporte (OSM)
    print("   🚌 Descargando Transporte (OSM)...")
    gdf_transit = get_transit_osm(bbox)

    # B. Descargar POIs (PostGIS)
    google_pois = get_google_pois_from_db(city)

    # C. Calcular Distancias
    total = len(df_city)
    print(f"   🚀 Calculando OSRM para {total} hexágonos...")

    # Inicializar columnas por defecto
    df_city['dist_cafe'] = 5000
    df_city['dist_gym'] = 5000
    df_city['dist_shop'] = 5000
    df_city['dist_transit'] = 5000

    for idx, row in df_city.iterrows():
        if idx % 100 == 0: print(f"      {idx}/{total}...", end="\r")
        
        # Cafe
        if 'cafe' in google_pois:
            df_city.at[idx, 'dist_cafe'] = get_osrm_dist(row['lat'], row['lon'], google_pois['cafe'])
        
        # Gimnasio
        if 'gym' in google_pois:
            df_city.at[idx, 'dist_gym'] = get_osrm_dist(row['lat'], row['lon'], google_pois['gym'])
            
        # Tiendas
        if 'shop' in google_pois:
            df_city.at[idx, 'dist_shop'] = get_osrm_dist(row['lat'], row['lon'], google_pois['shop'])
            
        # Transporte Público
        if not gdf_transit.empty:
            df_city.at[idx, 'dist_transit'] = get_osrm_dist(row['lat'], row['lon'], gdf_transit)

    dfs_result.append(df_city)

# 3. Guardado Inteligente
if dfs_result:
    df_batch = pd.concat(dfs_result)
    
    if os.path.exists(OUTPUT_FINAL_PATH):
        df_old = pd.read_csv(OUTPUT_FINAL_PATH)
        # Borramos lo viejo de estas ciudades y metemos lo nuevo
        df_final = df_old[~df_old['city'].isin(cities_to_process)]
        df_final = pd.concat([df_final, df_batch], ignore_index=True)
    else:
        df_final = df_batch
    
    os.makedirs(os.path.dirname(OUTPUT_FINAL_PATH), exist_ok=True)
    df_final.to_csv(OUTPUT_FINAL_PATH, index=False)
    print(f"\n✅ Dataset Final Actualizado: {OUTPUT_FINAL_PATH}")
else:
    print("\n✅ Nada procesado.")
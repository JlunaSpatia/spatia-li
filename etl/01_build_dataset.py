import osmnx as ox
import geopandas as gpd
import pandas as pd
import h3
import requests
from shapely.geometry import Point
from sqlalchemy import create_engine
import warnings
import sys
import os

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import TARGET_CITIES, H3_RESOLUTION, OSRM_WALK_URL

# Configuración de Conexión a PostGIS (Según tu Manual de Ops)
DB_CONNECTION = "postgresql://postgres:postgres@localhost:5432/spatia"

warnings.filterwarnings("ignore")
CSV_PATH = "etl/final_dataset.csv"

print(f"🚀 PASO 01: GESTIÓN DE CIUDADES (HÍBRIDO OSM + GOOGLE)...")

# --- 1. LÓGICA DE DELTA ---
cities_to_process = []
df_existing = pd.DataFrame()

if os.path.exists(CSV_PATH):
    print("   📂 Encontrado dataset previo. Analizando...")
    df_existing = pd.read_csv(CSV_PATH)
    existing_cities = df_existing['city'].unique().tolist()
    
    for target in TARGET_CITIES:
        clean_target_name = target.split(",")[0]
        if clean_target_name not in existing_cities:
            cities_to_process.append(target)
else:
    print("   🆕 No hay datos previos. Se procesará todo.")
    cities_to_process = TARGET_CITIES

if not cities_to_process:
    print("   ✅ Nada nuevo que procesar.")
    exit()

print(f"   🔨 Ciudades a procesar: {cities_to_process}")

# --- 2. FUNCIONES DE APOYO ---

def get_hexagons_from_city(city_query):
    # (Misma función de siempre para generar H3)
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
    # Calcula distancia a pie usando OSRM local
    if dest_gdf.empty: return 5000
    origin_point = Point(origin_lon, origin_lat)
    
    # 1. Filtro rápido euclidiano (para no pedir ruta a un punto en la otra punta de la ciudad)
    # Copiamos para no afectar el original
    dest_gdf_calc = dest_gdf.copy()
    dest_gdf_calc['temp_dist'] = dest_gdf_calc.distance(origin_point)
    nearest = dest_gdf_calc.sort_values('temp_dist').iloc[0]
    
    # 2. Petición OSRM al puerto 5001 (Walking)
    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest.geometry.x},{nearest.geometry.y}?overview=false"
    try:
        r = requests.get(url, timeout=2) # Timeout para no colgarse
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res and len(res['routes']) > 0:
                return res['routes'][0]['duration'] # Segundos
    except: pass
    return 5000

def get_google_pois_from_db(city_name):
    """Descarga POIs de Google desde PostGIS filtrando por snapshot más reciente"""
    print(f"      🛢️ Consultando PostGIS para {city_name} (Google Data)...")
    
    # Mapeo de Categorías de Google a nuestras columnas
    # Ajusta esto según tus necesidades de negocio
    category_map = {
    # --- GRUPO 1: Ocio y Gastronomía (Variable: dist_cafe) ---
    'Cafetería': 'cafe',
    'Bar': 'cafe',
    'Panadería': 'cafe',       # A veces la gente desayuna aquí
    'Restaurante': 'cafe',     # IMPORTANTE: No lo tenías, hay que añadirlo
    #'Comida rápida': 'cafe',   # IMPORTANTE: Genera mucho tráfico (target Burger King)

    # --- GRUPO 2: Salud y Físico (Variable: dist_gym) ---
    'Gimnasio': 'gym',
    # Nota: 'Tienda de deportes' la muevo a shop, porque es comprar, no sudar.

    # --- GRUPO 3: Retail y Compras (Variable: dist_shop) ---
    'Tienda de ropa': 'shop',
    'Centro comercial': 'shop', # El "Rey" del retail
    'Tienda de deportes': 'shop', 
    
    # --- ¿Qué hacemos con los "Esenciales"? ---
    # Opción A: Meterlos en shop (Diluye el concepto de "Moda" pero cuenta como comercio)
    #'Supermercado': 'shop',
    #'Farmacia': 'shop'
    }
    categories_sql = "', '".join(category_map.keys())
    
    # Query SQL optimizada: Solo trae lo más nuevo de esa ciudad y categorías relevantes
    query = f"""
    SELECT latitude, longitude, search_category
    FROM public.retail_poi_master
    WHERE city = '{city_name.upper()}' 
      AND snapshot_date = (
          SELECT MAX(snapshot_date) 
          FROM public.retail_poi_master 
          WHERE city = '{city_name.upper()}'
      )
      AND search_category IN ('{categories_sql}')
    """
    
    engine = create_engine(DB_CONNECTION)
    try:
        df_db = pd.read_sql(query, engine)
        if df_db.empty:
            print(f"      ⚠️ ADVERTENCIA: No hay datos de Google para {city_name} en PostGIS.")
            return {}
            
        # Convertir a GeoDataFrame
        geometry = [Point(xy) for xy in zip(df_db.longitude, df_db.latitude)]
        gdf_google = gpd.GeoDataFrame(df_db, geometry=geometry, crs="EPSG:4326")
        
        # Separar en diccionarios por tipo interno ('cafe', 'gym', etc.)
        pois_dict = {}
        df_db['internal_type'] = df_db['search_category'].map(category_map)
        
        for p_type in df_db['internal_type'].unique():
            pois_dict[p_type] = gdf_google[df_db['internal_type'] == p_type]
            
        return pois_dict
        
    except Exception as e:
        print(f"      ❌ Error conectando a PostGIS: {e}")
        return {}

# --- 3. PROCESAMIENTO ---
new_city_dfs = []

# Definimos tags SOLO para Transit (OSM)
osm_tags = {"transit": {"highway": "bus_stop", "railway": "subway_entrance"}}

for city in cities_to_process:
    # A. Generar Hexágonos
    df_hex = get_hexagons_from_city(city)
    if df_hex.empty: continue
    
    clean_city_name = city.split(",")[0] # "Madrid"
    
    # B. Obtener Datos (Híbrido)
    # 1. Transit desde OSM (Geometría del área)
    try:
        area_polygon = ox.geocode_to_gdf(city).geometry[0]
        gdf_transit = ox.features_from_polygon(area_polygon, tags=osm_tags['transit'])
        gdf_transit['geometry'] = gdf_transit.geometry.centroid # Asegurar puntos
    except:
        gdf_transit = gpd.GeoDataFrame()
        
    # 2. Lifestyle desde Google (PostGIS)
    google_pois = get_google_pois_from_db(clean_city_name) # Devuelve dict {'cafe': gdf, 'gym': gdf...}
    
    # C. Calcular Distancias
    print(f"      🚀 Calculando OSRM para {len(df_hex)} hexágonos en {clean_city_name}...")
    
    # Iteramos (Optimización: Podríamos vectorizar, pero para MVP el loop sirve)
    for idx, row in df_hex.iterrows():
        if idx % 100 == 0: print(f"         {idx}/{len(df_hex)}...", end="\r")
        
        # Distancias a Google POIs
        df_hex.at[idx, 'dist_cafe'] = get_osrm_dist(row['lat'], row['lon'], google_pois.get('cafe', gpd.GeoDataFrame()))
        df_hex.at[idx, 'dist_gym'] = get_osrm_dist(row['lat'], row['lon'], google_pois.get('gym', gpd.GeoDataFrame()))
        df_hex.at[idx, 'dist_shop'] = get_osrm_dist(row['lat'], row['lon'], google_pois.get('shop', gpd.GeoDataFrame()))
        
        # Distancia a OSM Transit
        df_hex.at[idx, 'dist_transit'] = get_osrm_dist(row['lat'], row['lon'], gdf_transit)

    new_city_dfs.append(df_hex)

# --- 4. GUARDADO ---
if new_city_dfs:
    df_new_batch = pd.concat(new_city_dfs).reset_index(drop=True)
    
    if not df_existing.empty:
        df_final = pd.concat([df_existing, df_new_batch], ignore_index=True)
    else:
        df_final = df_new_batch

    df_final.to_csv(CSV_PATH, index=False)
    print(f"\n✅ DATASET ACTUALIZADO CON DATOS HÍBRIDOS (Google + OSM).")
else:
    print("\n❌ No se generaron datos nuevos.")
import osmnx as ox
import geopandas as gpd
import pandas as pd
import h3
import requests
from shapely.geometry import Polygon, Point
from sqlalchemy import create_engine
from rasterstats import zonal_stats
import warnings
import sys
import os

# ==========================================
# 1. SETUP DE RUTAS Y CONFIG
# ==========================================
# Ubicación actual: .../spatia-li/etl/01_build_dataset.py
current_dir = os.path.dirname(os.path.abspath(__file__))

# CORRECCIÓN: Subimos solo 1 nivel para llegar a la raíz (spatia-li)
project_root = os.path.dirname(current_dir) 
sys.path.append(project_root)

# Importamos la configuración centralizada
try:
    from config import (
        DB_CONNECTION_STR, 
        DATA_DIR,          # "data/raw"
        TARGET_CITIES,     # ["Madrid, Spain"]
        H3_RESOLUTION,     # 9
        OSRM_WALK_URL      # "http://localhost:5001"
    )
except ImportError:
    print("❌ Error Crítico: No encuentro 'config.py' en la raíz del proyecto.")
    print(f"   Ruta buscada: {project_root}/config.py")
    sys.exit(1)

warnings.filterwarnings("ignore")

# Rutas de Archivos
CSV_PATH = os.path.join(project_root, "data", "processed", "final_dataset.csv")

# ⚠️ CONFIGURA AQUÍ EL NOMBRE DE TU TIF ⚠️
# Asegúrate de que este archivo está dentro de la carpeta spatia-li/data/raw/
GHS_FILENAME = "GHS_BUILT_S_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"  # <--- CAMBIA ESTO SI SE LLAMA DIFERENTE
GHS_PATH = os.path.join(project_root, DATA_DIR, GHS_FILENAME)

# ==========================================
# 2. FUNCIONES
# ==========================================

def filter_by_urban_footprint(df_hex):
    """
    Usa el raster GHS_BUILT para eliminar hexágonos donde no hay construcción.
    """
    if not os.path.exists(GHS_PATH):
        print(f"      ⚠️ ALERTA: No encuentro el raster GHS en {GHS_PATH}.")
        print("      -> Saltando filtro urbano (se procesarán zonas rurales).")
        return df_hex

    print(f"      🏗️ Filtrando zonas no urbanas (Raster GHS)...")
    
    # Convertir H3 a Polígonos para rasterstats
    hex_polygons = []
    for h in df_hex['h3_index']:
        geo_json = h3.h3_to_geo_boundary(h, geo_json=True)
        # H3 devuelve (lon, lat), Shapely necesita eso mismo
        poly = Polygon(geo_json)
        hex_polygons.append(poly)
    
    # GeoDataFrame temporal
    gdf_temp = gpd.GeoDataFrame({'h3_index': df_hex['h3_index']}, geometry=hex_polygons, crs="EPSG:4326")
    
    # Calcular SUMA de píxeles construidos
    # stats devolverá una lista de dicts: [{'sum': 120.5}, {'sum': 0.0}, ...]
    stats = zonal_stats(
        vectors=gdf_temp['geometry'],
        raster=GHS_PATH,
        stats=['sum']
    )
    
    # Extraer valores (si es None por estar fuera del mapa, ponemos 0)
    built_values = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    df_hex['built_up_score'] = built_values
    
    # FILTRO: Si built_up_score es 0 (o muy cercano), es campo.
    initial_count = len(df_hex)
    df_filtered = df_hex[df_hex['built_up_score'] > 50].copy()
    
    removed = initial_count - len(df_filtered)
    print(f"      ✂️ Eliminados {removed} hexágonos rurales (Quedan {len(df_filtered)})")
    
    return df_filtered

def get_hexagons_from_city(city_query):
    try:
        gdf_city = ox.geocode_to_gdf(city_query)
    except: return pd.DataFrame()

    gdf_exploded = gdf_city.explode(index_parts=False)
    all_hexagons = set()
    for _, row in gdf_exploded.iterrows():
        try:
            if row.geometry.geom_type == 'Polygon':
                # USAMOS LA VARIABLE DE CONFIG
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
    
    dest_gdf_calc = dest_gdf.copy()
    dest_gdf_calc['temp_dist'] = dest_gdf_calc.distance(origin_point)
    nearest = dest_gdf_calc.sort_values('temp_dist').iloc[0]
    
    # Filtro rápido euclidiano (aprox 5km)
    if nearest['temp_dist'] > 0.05: return 5000 

    # USAMOS LA URL DEL CONFIG
    url = f"{OSRM_WALK_URL}/route/v1/foot/{origin_lon},{origin_lat};{nearest.geometry.x},{nearest.geometry.y}?overview=false"
    try:
        r = requests.get(url, timeout=0.5) 
        if r.status_code == 200:
            res = r.json()
            if 'routes' in res and len(res['routes']) > 0:
                return res['routes'][0]['duration']
    except: pass
    return 5000

def get_google_pois_from_db(city_name):
    print(f"      🛢️ Consultando PostGIS para {city_name}...")
    
    category_map = {
        'Cafetería': 'cafe', 'Bar': 'cafe', 'Panadería': 'cafe', 'Restaurante': 'cafe',
        'Gimnasio': 'gym',
        'Tienda de ropa': 'shop', 'Centro comercial': 'shop', 'Tienda de deportes': 'shop'
    }
    categories_sql = "', '".join(category_map.keys())
    
    # Buscamos por la columna 'snapshot_date' más reciente
    query = f"""
    SELECT latitude, longitude, search_category
    FROM public.retail_poi_master
    WHERE UPPER(city) = '{city_name.upper()}' 
      AND snapshot_date = (
          SELECT MAX(snapshot_date) FROM public.retail_poi_master WHERE UPPER(city) = '{city_name.upper()}'
      )
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
        
    except Exception as e:
        print(f"      ❌ Error conectando a PostGIS: {e}")
        return {}

# ==========================================
# 3. PROCESO PRINCIPAL
# ==========================================

print(f"🚀 PASO 01: GENERACIÓN DE DATASET (Master)...")

# 1. LÓGICA DE DELTA (¿Qué ciudades faltan?)
cities_to_process = []
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

if os.path.exists(CSV_PATH):
    print("   📂 Dataset previo detectado.")
    df_existing = pd.read_csv(CSV_PATH)
    existing_cities = df_existing['city'].unique().tolist()
    
    for target in TARGET_CITIES:
        clean_target_name = target.split(",")[0]
        if clean_target_name not in existing_cities:
            cities_to_process.append(target)
else:
    print("   🆕 Creando dataset desde cero.")
    cities_to_process = TARGET_CITIES
    df_existing = pd.DataFrame()

if not cities_to_process:
    print("   ✅ Todas las ciudades del config ya están procesadas.")
    sys.exit()

print(f"   🔨 Procesando: {cities_to_process}")

# 2. BUCLE DE PROCESAMIENTO
new_city_dfs = []
osm_tags = {"transit": {"highway": "bus_stop", "railway": "subway_entrance"}}

for city in cities_to_process:
    print(f"\n🏙️  {city}")
    
    # A. Hexágonos
    df_hex = get_hexagons_from_city(city)
    if df_hex.empty: 
        print("      ⚠️ Error generando hexágonos.")
        continue
    
    print(f"      ⬡ Brutos: {len(df_hex)}")

    # B. Filtro Urbano (GHS)
    df_hex = filter_by_urban_footprint(df_hex)
    if df_hex.empty:
        print("      ⚠️ Todo filtrado. Saltando ciudad.")
        continue

    clean_city_name = city.split(",")[0]

    # C. Datos Contextuales
    try:
        area_polygon = ox.geocode_to_gdf(city).geometry[0]
        gdf_transit = ox.features_from_polygon(area_polygon, tags=osm_tags['transit'])
        gdf_transit['geometry'] = gdf_transit.geometry.centroid
    except:
        gdf_transit = gpd.GeoDataFrame()
        
    google_pois = get_google_pois_from_db(clean_city_name)
    
    # D. Distancias (OSRM)
    print(f"      🚀 Calculando rutas para {len(df_hex)} hexágonos...")
    total = len(df_hex)
    
    # Creamos columnas vacías para evitar errores si no hay datos
    df_hex['dist_cafe'] = 5000
    df_hex['dist_gym'] = 5000
    df_hex['dist_shop'] = 5000
    df_hex['dist_transit'] = 5000

    for idx, row in df_hex.iterrows():
        if idx % 500 == 0: print(f"         {idx}/{total}...", end="\r")
        
        # Solo calculamos si tenemos POIs de ese tipo
        if 'cafe' in google_pois:
            df_hex.at[idx, 'dist_cafe'] = get_osrm_dist(row['lat'], row['lon'], google_pois['cafe'])
        
        if 'gym' in google_pois:
            df_hex.at[idx, 'dist_gym'] = get_osrm_dist(row['lat'], row['lon'], google_pois['gym'])
            
        if 'shop' in google_pois:
            df_hex.at[idx, 'dist_shop'] = get_osrm_dist(row['lat'], row['lon'], google_pois['shop'])
            
        if not gdf_transit.empty:
            df_hex.at[idx, 'dist_transit'] = get_osrm_dist(row['lat'], row['lon'], gdf_transit)

    new_city_dfs.append(df_hex)

# 3. GUARDADO FINAL
if new_city_dfs:
    df_new_batch = pd.concat(new_city_dfs).reset_index(drop=True)
    
    if not df_existing.empty:
        df_final = pd.concat([df_existing, df_new_batch], ignore_index=True)
    else:
        df_final = df_new_batch

    df_final.to_csv(CSV_PATH, index=False)
    print(f"\n✅ DATASET ACTUALIZADO: {CSV_PATH}")
else:
    print("\n❌ No se generaron datos nuevos.")
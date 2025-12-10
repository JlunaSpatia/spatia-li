import geopandas as gpd
import pandas as pd
import osmnx as ox
from sqlalchemy import create_engine, text
from shapely.geometry import Point
import os
import sys
import warnings

# ==========================================
# 1. SETUP
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir)) 
sys.path.append(project_root)

# Ignorar alertas de proyección de GeoPandas
warnings.filterwarnings("ignore")

try:
    from config import DB_CONNECTION_STR, ACTIVE_CITIES
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"
    ACTIVE_CITIES = ["MADRID"]

EXPORT_DIR = os.path.join(project_root, "data", "exports")

def get_engine():
    return create_engine(DB_CONNECTION_STR)

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

# ==========================================
# 2. FUNCIONES ROBUSTAS
# ==========================================

def get_google_pois_gdf(city_name, engine):
    print(f"      🛢️ Extrayendo Google POIs...")
    
    # 1. Averiguar qué columnas tiene la tabla real
    try:
        check_cols = pd.read_sql(f"SELECT * FROM retail_poi_master LIMIT 0", engine)
        cols = [c.lower() for c in check_cols.columns]
    except:
        print("      ⚠️ No existe la tabla 'retail_poi_master'.")
        return gpd.GeoDataFrame()

    # 2. Detectar cuál es la columna de nombre
    name_col = 'name'
    if 'name' not in cols:
        if 'title' in cols: name_col = 'title'
        elif 'place_name' in cols: name_col = 'place_name'
        else:
            print(f"      ❌ No encuentro columna de nombre. Columnas disponibles: {cols}")
            return gpd.GeoDataFrame()

    # 3. Consulta Inteligente (Case Insensitive para la ciudad)
    # Usamos COALESCE para coger la fecha más reciente disponible
    sql = f"""
    SELECT {name_col} as name, latitude, longitude, search_category
    FROM public.retail_poi_master
    WHERE UPPER(city) = '{city_name.upper()}'
    """
    
    try:
        df = pd.read_sql(sql, engine)
        if df.empty: return gpd.GeoDataFrame()

        # Mapeo de categorías
        category_map = {
            'Cafetería': 'cafe', 'Bar': 'cafe', 'Panadería': 'cafe', 'Restaurante': 'cafe',
            'Gimnasio': 'gym', 'Tienda de ropa': 'shop', 'Centro comercial': 'shop', 
            'Tienda de deportes': 'shop', 'Supermercado': 'shop'
        }
        
        # Si search_category no coincide, lo dejamos como 'other'
        df['type'] = df['search_category'].map(category_map).fillna('other')
        df['source'] = 'google'
        
        geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        return gdf[['name', 'type', 'source', 'geometry']]
        
    except Exception as e:
        print(f"      ❌ Error SQL Google: {e}")
        return gpd.GeoDataFrame()

def get_osm_transit_gdf(city_name_full):
    print(f"      🌍 Descargando OSM Transit (live)...")
    osm_tags = {"highway": "bus_stop", "railway": "subway_entrance", "public_transport": "station"}
    
    try:
        # Usamos geocode para obtener el área
        gdf = ox.features_from_place(city_name_full, tags=osm_tags)
        
        if gdf.empty: return gpd.GeoDataFrame()
        
        # Convertir polígonos a puntos y suprimir warning
        gdf['geometry'] = gdf.geometry.centroid
        
        # Limpieza
        if 'name' not in gdf.columns: gdf['name'] = 'Unknown'
        gdf['name'] = gdf['name'].fillna('Bus/Metro Stop')
        gdf['type'] = 'transit'
        gdf['source'] = 'osm'
        
        return gdf[['name', 'type', 'source', 'geometry']]
        
    except Exception as e:
        # A veces OSM falla si no encuentra el lugar exacto
        print(f"      ⚠️ OSM Info: {e}")
        return gpd.GeoDataFrame()

def get_hexagons_gdf(city_name, engine):
    print(f"      ⬡ Leyendo Hexágonos...")
    
    # Comprobamos primero si la tabla existe y tiene datos
    try:
        count = pd.read_sql(f"SELECT count(*) FROM retail_hexagons_enriched WHERE UPPER(city)='{city_name.upper()}'", engine).iloc[0,0]
        if count == 0:
            print(f"      ⚠️ La tabla está vacía para {city_name}. (¿Ejecutaste 03_enrich?)")
            return gpd.GeoDataFrame()
    except:
        # Si falla el count, intentamos leer la tabla raw
        print("      ⚠️ Tabla 'enriched' no encontrada. Probando 'retail_hexagons' (raw)...")
        try:
             sql = f"SELECT h3_index, geometry FROM retail_hexagons WHERE UPPER(city)='{city_name.upper()}'"
             return gpd.read_postgis(sql, engine, geom_col='geometry')
        except:
             return gpd.GeoDataFrame()

    # Si todo va bien, leemos la completa
    sql = f"""
        SELECT h3_index, avg_income, target_pop, dist_cafe, dist_gym, dist_transit, geometry 
        FROM retail_hexagons_enriched 
        WHERE UPPER(city) = '{city_name.upper()}'
    """
    try:
        gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
        gdf.rename(columns={'avg_income': 'renta', 'target_pop': 'poblacion', 'dist_cafe': 'd_cafe'}, inplace=True)
        return gdf
    except Exception as e:
        print(f"      ❌ Error leyendo Hexágonos: {e}")
        return gpd.GeoDataFrame()

# ==========================================
# 3. EJECUCIÓN
# ==========================================
def run_export():
    print("💾 EXPORTACIÓN DE CAPAS (QA V3)...")
    engine = get_engine()
    ensure_directory(EXPORT_DIR)

    for city in ACTIVE_CITIES:
        print(f"\n🏙️  {city}")
        city_folder = os.path.join(EXPORT_DIR, city)
        ensure_directory(city_folder)
        
        # 1. Hexágonos
        gdf_h = get_hexagons_gdf(city, engine)
        if not gdf_h.empty:
            gdf_h.to_file(os.path.join(city_folder, f"{city}_hexagons.shp"))
            print(f"   ✅ Hexágonos: {len(gdf_h)}")
        
        # 2. Google
        gdf_g = get_google_pois_gdf(city, engine)
        if not gdf_g.empty:
            gdf_g.to_file(os.path.join(city_folder, f"{city}_google.shp"))
            print(f"   ✅ Google: {len(gdf_g)}")
            
        # 3. OSM
        gdf_o = get_osm_transit_gdf(f"{city}, Spain")
        if not gdf_o.empty:
            gdf_o.to_file(os.path.join(city_folder, f"{city}_osm_transit.shp"))
            print(f"   ✅ OSM Transit: {len(gdf_o)}")

if __name__ == "__main__":
    run_export()
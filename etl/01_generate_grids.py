import pandas as pd
import geopandas as gpd
import h3
from shapely.geometry import Polygon, box
from rasterstats import zonal_stats
import sys
import os
import warnings

# ================= SETUP =================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Importamos la configuración y el filtro ACTIVE_CITIES
from config import CITY_BBOXES, DATA_DIR, H3_RESOLUTION, ACTIVE_CITIES

# Rutas de Salida
OUTPUT_GRID_PATH = os.path.join(project_root, "data", "processed", "01_urban_grid.csv")
GHS_FILENAME = "GHS_BUILT_S_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"
GHS_PATH = os.path.join(project_root, DATA_DIR, GHS_FILENAME)

warnings.filterwarnings("ignore")

# ================= FUNCIONES =================

def get_hexagons_from_bbox(city_name, bbox_dict):
    """Genera hexágonos dentro del rectángulo definido en config."""
    print(f"   ⬡ Generando rejilla H3 (Res {H3_RESOLUTION}) para {city_name}...")
    
    # Crear rectángulo Shapely (lon_min, lat_min, lon_max, lat_max)
    bbox_poly = box(
        bbox_dict['min_lon'], bbox_dict['min_lat'], 
        bbox_dict['max_lon'], bbox_dict['max_lat']
    )
    
    try:
        # Polyfill rellena el polígono con hexágonos
        hexs = h3.polyfill(bbox_poly.__geo_interface__, H3_RESOLUTION, geo_json_conformant=True)
    except Exception as e:
        print(f"❌ Error H3 Polyfill: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(list(hexs), columns=['h3_index'])
    df['city'] = city_name
    # Calculamos centroides para uso posterior
    df['lat'] = df['h3_index'].apply(lambda x: h3.h3_to_geo(x)[0])
    df['lon'] = df['h3_index'].apply(lambda x: h3.h3_to_geo(x)[1])
    return df

def filter_by_urban_footprint(df_hex):
    """Elimina hexágonos que caen en zonas rurales según Raster GHS."""
    if not os.path.exists(GHS_PATH):
        print(f"      ⚠️ Raster GHS no encontrado en {GHS_PATH}. Saltando filtro.")
        df_hex['built_up_score'] = 100 # Asumimos urbano si no hay raster
        return df_hex

    print(f"      🏗️ Cruzando con Raster GHS...")
    # Convertir a geometrías para rasterstats
    hex_polygons = [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in df_hex['h3_index']]
    gdf_temp = gpd.GeoDataFrame({'h3_index': df_hex['h3_index']}, geometry=hex_polygons, crs="EPSG:4326")
    
    # Calcular 'sum' de píxeles construidos dentro de cada hexágono
    stats = zonal_stats(vectors=gdf_temp['geometry'], raster=GHS_PATH, stats=['sum'])
    built_values = [x['sum'] if x['sum'] is not None else 0 for x in stats]
    
    df_hex['built_up_score'] = built_values
    
    # Filtro: debe tener algo de construcción (>50 es un valor empírico seguro para res 9)
    df_filtered = df_hex[df_hex['built_up_score'] > 50].copy()
    
    removed = len(df_hex) - len(df_filtered)
    print(f"      ✂️ Eliminados {removed} hexágonos rurales (Quedan {len(df_filtered)})")
    return df_filtered

# ================= MAIN =================

print("🚀 INICIO: Generación de Malla Hexagonal (Paso 1)")

# 1. Determinar qué ciudades procesar
if ACTIVE_CITIES:
    cities_to_process = [c for c in ACTIVE_CITIES if c in CITY_BBOXES]
    print(f"🎯 MODO FILTRO ACTIVADO: Procesando solo {cities_to_process}")
else:
    cities_to_process = list(CITY_BBOXES.keys())
    print(f"🌍 MODO COMPLETO: Procesando todas las ciudades.")

dfs_new = []

# 2. Bucle principal
for city_name in cities_to_process:
    print(f"\n🏙️  Procesando: {city_name}")
    bbox = CITY_BBOXES[city_name]
    
    # A. Generar
    df_hex = get_hexagons_from_bbox(city_name, bbox)
    if df_hex.empty: continue
    
    # B. Filtrar
    df_urban = filter_by_urban_footprint(df_hex)
    dfs_new.append(df_urban)

# 3. Guardado Inteligente (Actualiza lo existente sin borrar otras ciudades)
if dfs_new:
    df_batch = pd.concat(dfs_new)
    
    if os.path.exists(OUTPUT_GRID_PATH):
        print("   📂 Actualizando archivo existente...")
        df_old = pd.read_csv(OUTPUT_GRID_PATH)
        # Eliminamos del archivo viejo las ciudades que acabamos de recalcular para sobrescribirlas
        df_final = df_old[~df_old['city'].isin(cities_to_process)]
        df_final = pd.concat([df_final, df_batch], ignore_index=True)
    else:
        df_final = df_batch
        
    os.makedirs(os.path.dirname(OUTPUT_GRID_PATH), exist_ok=True)
    df_final.to_csv(OUTPUT_GRID_PATH, index=False)
    print(f"\n✅ Malla guardada en: {OUTPUT_GRID_PATH}")
else:
    print("\n⚠️ No se generaron datos nuevos.")
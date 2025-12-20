import sys
import os
import h3
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, box, mapping
from sqlalchemy import create_engine, text, inspect
from rasterstats import zonal_stats  # <--- NECESARIO: pip install rasterstats

# 1. Configuración de rutas
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Importación segura
try:
    from conf import DB_URL, CITY_BBOXES, ACTIVE_CITIES, H3_RESOLUTION
except ImportError:
    print("❌ Error: No se encuentra 'conf.py'.")
    sys.exit(1)

# --- CONFIGURACIÓN RASTER ---
# Ajusta el nombre si tu archivo se llama diferente
GHS_FILENAME = "GHS_BUILT_S_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"
GHS_PATH = os.path.join(BASE_DIR, "data", "raw", GHS_FILENAME)

def get_existing_cities(engine):
    """Devuelve un set con los nombres de las ciudades que YA están en la BBDD."""
    inspector = inspect(engine)
    if not inspector.has_table("hexagons", schema="core"):
        return set()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT city FROM core.hexagons"))
            return {row[0] for row in result}
    except Exception as e:
        print(f"⚠️ No se pudo consultar ciudades existentes: {e}")
        return set()

def filter_by_urban_footprint(df_hex, city_name):
    """
    Filtra hexágonos que caen en zonas sin edificación según el Raster GHS.
    """
    # 1. Si no existe el raster, devolvemos todo (sin filtrar)
    if not os.path.exists(GHS_PATH):
        print(f"      ⚠️ Raster GHS no encontrado en {GHS_PATH}. Saltando filtro poblacional.")
        return df_hex

    # 2. Convertir a GeoDataFrame si no lo es (necesario para zonal_stats)
    if not isinstance(df_hex, gpd.GeoDataFrame):
        gdf_temp = gpd.GeoDataFrame(df_hex, geometry='geometry', crs="EPSG:4326")
    else:
        gdf_temp = df_hex

    print(f"      🏘️ Filtrando zonas deshabitadas (Raster GHS)...")
    
    # 3. Zonal Stats: Sumamos el valor de píxeles construidos dentro del hexágono
    try:
        stats = zonal_stats(
            vectors=gdf_temp['geometry'], 
            raster=GHS_PATH, 
            stats=['sum']
        )
        
        # Asignamos score (0 si es None)
        df_hex['built_up_score'] = [x['sum'] if x['sum'] is not None else 0 for x in stats]
        
        # 4. FILTRO: Score > 50 (Umbral empírico de zona urbana)
        # Esto elimina hexágonos en medio del bosque o mar
        df_filtered = df_hex[df_hex['built_up_score'] > 50].copy()
        
        dropped = len(df_hex) - len(df_filtered)
        print(f"      ✂️ Eliminados {dropped} hexágonos vacíos/rurales.")
        
        # Limpiamos columna temporal
        return df_filtered.drop(columns=['built_up_score'])
        
    except Exception as e:
        print(f"      ⚠️ Error en filtro raster: {e}. Se mantienen todos.")
        return df_hex

def generate_master_grid():
    engine = create_engine(DB_URL)
    print(f"🌍 Iniciando Grid Master (Res: {H3_RESOLUTION})...")
    
    # 1. VERIFICAR QUÉ TENEMOS YA
    existing_cities = get_existing_cities(engine)
    if existing_cities:
        print(f"   💾 Ciudades ya existentes en BBDD: {', '.join(existing_cities)}")
    else:
        print("   💾 La base de datos está limpia (o tabla no existe).")

    # Determinar objetivos
    target_cities = ACTIVE_CITIES if ACTIVE_CITIES else CITY_BBOXES.keys()
    
    hexagons_to_add = []
    
    # 2. PROCESAR CADA CIUDAD
    for city in target_cities:
        if city not in CITY_BBOXES:
            print(f"⚠️ La ciudad {city} no tiene configuración en conf.py. Saltando.")
            continue
            
        if city in existing_cities:
            print(f"   ⏭️  SKIPPING: {city} ya existe en la base de datos.")
            continue
            
        print(f"   📍 Generando Hexágonos para: {city}...")
        coords = CITY_BBOXES[city]
        
        # Generar Polígono BBOX
        city_polygon = box(coords['min_lon'], coords['min_lat'], coords['max_lon'], coords['max_lat'])
        geo_json = mapping(city_polygon)
        
        # Generar IDs H3
        hex_ids = h3.polyfill(geo_json, H3_RESOLUTION, geo_json_conformant=True)
        
        if not hex_ids:
            print(f"      ⚠️ Zona vacía o muy pequeña para resolución {H3_RESOLUTION}.")
            continue

        # Crear DF temporal
        df_city = pd.DataFrame(list(hex_ids), columns=['h3_id'])
        df_city['city'] = city
        
        # Generar geometría (Necesaria para el filtro raster)
        df_city['geometry'] = df_city['h3_id'].apply(
            lambda x: Polygon(h3.h3_to_geo_boundary(x, geo_json=True))
        )
        
        print(f"      ⬡ Hexágonos brutos generados: {len(df_city)}")

        # --- APLICAR FILTRO DE POBLACIÓN (NUEVO) ---
        df_city = filter_by_urban_footprint(df_city, city)
        
        if df_city.empty:
            print(f"      ⚠️ La ciudad {city} se quedó sin hexágonos tras el filtro.")
            continue

        hexagons_to_add.append(df_city)

    # 3. GUARDAR (APPEND)
    if not hexagons_to_add:
        print("✅ No hay ciudades nuevas para añadir. Todo está actualizado.")
        return

    print("   🔄 Unificando nuevos datos...")
    final_df = pd.concat(hexagons_to_add, ignore_index=True)
    gdf_hex = gpd.GeoDataFrame(final_df, geometry='geometry', crs="EPSG:4326")
    
    print(f"💾 Añadiendo {len(gdf_hex)} hexágonos nuevos a 'core.hexagons'...")
    
    try:
        gdf_hex.to_postgis(
            name="hexagons",
            con=engine,
            schema="core",
            if_exists="append", 
            index=False 
        )
        
        # Asegurar índices
        with engine.connect() as con:
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_hex_h3 ON core.hexagons (h3_id);"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_hex_city ON core.hexagons (city);"))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_hex_geom ON core.hexagons USING GIST (geometry);"))
            con.commit()
            
        print("✅ ÉXITO: Grid Maestro actualizado.")
        
    except Exception as e:
        print(f"❌ Error guardando en BBDD: {e}")
        if "duplicate key" in str(e):
            print("   (Revisa solapamientos entre las cajas de las ciudades)")

if __name__ == "__main__":
    generate_master_grid()
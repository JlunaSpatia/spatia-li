import sys
import os
import re
import gc
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from shapely.geometry import box

# 1. Rutas
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL, ACTIVE_CITIES
except ImportError:
    print("❌ Error: No se encuentra 'conf.py'.")
    sys.exit(1)

# --- CONFIG ---
SHP_FILENAME = "SECC_CE_20230101.shp"
CSV_FILENAME = "INE_2023_Renta.csv"
TARGET_INDICATOR = "Renta bruta media por hogar" 
CALC_CRS = "EPSG:25830" 
CENSUS_SHP = os.path.join(BASE_DIR, "data", "raw", SHP_FILENAME)
RENTA_CSV = os.path.join(BASE_DIR, "data", "raw", CSV_FILENAME)
SCHEMA = "core"
TABLE = "census"

def clean_currency_ine(x):
    if pd.isna(x): return 0.0
    clean_str = str(x).replace('.', '').replace(',', '.')
    try: return float(clean_str)
    except: return 0.0

def load_census_layer():
    engine = create_engine(DB_URL)
    census_year = 2023
    print(f"🚀 INICIANDO CENSUS 6.0 (Equilibrio RAM/Velocidad)")

    # 1. CARGA CSV (Rápido)
    print("📥 [1/4] Leyendo CSV INE...")
    df_csv_raw = pd.read_csv(RENTA_CSV, sep=';', dtype=str, encoding='utf-8')
    col_ind = [c for c in df_csv_raw.columns if 'Indicadores' in c][0]
    mask = (df_csv_raw[col_ind].str.contains(TARGET_INDICATOR, case=False, na=False)) & \
           (pd.to_numeric(df_csv_raw['Periodo'], errors='coerce') == census_year)
    df_clean = df_csv_raw[mask].copy()
    df_clean['renta'] = df_clean['Total'].apply(clean_currency_ine)
    df_clean['CUSEC'] = df_clean['Secciones'].astype(str).str.strip().str[:10]
    df_renta = df_clean[df_clean['renta'] > 0][['CUSEC', 'renta']].copy()
    del df_csv_raw, df_clean
    gc.collect()

    # 2. CARGA GRID COMPLETO (Saber qué áreas necesitamos)
    print("📥 [2/4] Consultando áreas activas en PostGIS...")
    cities_str = "'" + "','".join(ACTIVE_CITIES) + "'"
    sql = text(f"SELECT h3_id, geometry, city FROM core.hexagons WHERE city IN ({cities_str})")
    gdf_all_hex = gpd.read_postgis(sql, engine, geom_col="geometry")
    
    if gdf_all_hex.empty:
        print("❌ No hay hexágonos para las ciudades activas.")
        return

    # 3. CARGA MAPA (Una sola vez para todas las ciudades activas)
    print("📥 [3/4] Cargando trozo de mapa nacional (Optimizado)...")
    try:
        # Detectamos el BBOX total que cubre TODAS las ciudades
        total_bounds = gdf_all_hex.total_bounds
        
        # Leemos el CRS primero
        map_crs = gpd.read_file(CENSUS_SHP, rows=1).crs
        
        # Transformamos el BBOX total al CRS del mapa para filtrar una sola vez
        bbox_trans = gpd.GeoSeries([box(*total_bounds)], crs="EPSG:4326").to_crs(map_crs).total_bounds
        
        # ESTA ES LA CLAVE: Leemos una vez el trozo que contiene Madrid, Barna y El Portil
        gdf_mapa_base = gpd.read_file(CENSUS_SHP, bbox=tuple(bbox_trans)).to_crs("EPSG:4326")
        
        # Limpieza de IDs en el mapa
        col_mapa = next((c for c in gdf_mapa_base.columns if c in ['CUSEC', 'CD_SECC']), None)
        gdf_mapa_base.rename(columns={col_mapa: 'CUSEC'}, inplace=True)
        gdf_mapa_base['CUSEC'] = gdf_mapa_base['CUSEC'].astype(str).str.strip()
        
        # Unimos con renta una sola vez
        gdf_census_base = gdf_mapa_base.merge(df_renta, on='CUSEC', how='inner')
        del gdf_mapa_base
        gc.collect()
        print(f"   ✅ Mapa preparado con {len(gdf_census_base)} secciones censales.")
        
    except Exception as e:
        print(f"❌ Error cargando mapa: {e}")
        return

    # 4. PROCESO POR CIUDAD (Ahora será instantáneo porque todo está en RAM)
    print("📥 [4/4] Procesando ciudades...")
    for city in ACTIVE_CITIES:
        print(f"\n   📍 {city.upper()}")
        
        # Filtramos hexágonos de la ciudad (Ya están en memoria)
        gdf_hex_city = gdf_all_hex[gdf_all_hex['city'] == city].copy()
        if gdf_hex_city.empty: continue

        # Recorte del mapa para esta ciudad (Instantáneo con .cx)
        b = gdf_hex_city.total_bounds
        gdf_census_city = gdf_census_base.cx[b[0]:b[2], b[1]:b[3]].copy()
        
        if gdf_census_city.empty:
            print("      ⚠️ Sin datos censales en esta zona.")
            continue

        # Interpolación (Lógica corregida de pesos)
        try:
            gdf_census_city = gdf_census_city.to_crs(CALC_CRS)
            gdf_hex_city = gdf_hex_city.to_crs(CALC_CRS)
            gdf_hex_city['area_hex'] = gdf_hex_city.geometry.area
            
            # Intersección
            overlay = gpd.overlay(gdf_census_city, gdf_hex_city, how='intersection')
            overlay['weight'] = overlay.geometry.area / overlay['area_hex']
            overlay['w_renta'] = overlay['renta'] * overlay['weight']
            
            result = overlay.groupby('h3_id')['w_renta'].sum().reset_index()
            result.rename(columns={'w_renta': 'renta_media_hogar'}, inplace=True)
            result['city'] = city
            result['year'] = census_year
            
            # Guardar en PostGIS
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {SCHEMA}.{TABLE} WHERE city = :city AND year = :year"), 
                             {"city": city, "year": census_year})
                conn.commit()

            result.to_sql(TABLE, engine, schema=SCHEMA, if_exists='append', index=False)
            print(f"      ✅ OK: {len(result)} hexágonos. Máx Renta: {result['renta_media_hogar'].max():.0f}€")

        except Exception as e:
            print(f"      ❌ Error espacial: {e}")

    print("\n🏁 ¡PROCESO COMPLETADO!")

if __name__ == "__main__":
    load_census_layer()
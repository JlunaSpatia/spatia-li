import sys
import os
import re
import gc
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from shapely.geometry import box

# 1. Configuración de rutas
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL, ACTIVE_CITIES
except ImportError:
    print("❌ Error: No se encuentra 'conf.py'.")
    sys.exit(1)

# --- CONFIGURACIÓN ---
SHP_FILENAME = "SECC_CE_20230101.shp"
CSV_FILENAME = "INE_2023_Renta.csv"
TARGET_INDICATOR = "Renta bruta media por hogar" 

CENSUS_SHP = os.path.join(BASE_DIR, "data", "raw", SHP_FILENAME)
RENTA_CSV = os.path.join(BASE_DIR, "data", "raw", CSV_FILENAME)

SCHEMA = "core"
TABLE = "census"

def extract_year_from_filename(filename):
    match = re.search(r'(\d{4})', filename)
    return int(match.group(1)) if match else None

def clean_currency_ine(x):
    if pd.isna(x): return 0.0
    clean_str = str(x).replace('.', '').replace(',', '.')
    try: return float(clean_str)
    except: return 0.0

def ensure_table_structure(engine):
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                h3_id TEXT PRIMARY KEY,
                renta_media_hogar FLOAT,
                city TEXT,
                year INTEGER
            );
        """))
        try:
            conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE} ADD COLUMN IF NOT EXISTS city TEXT;"))
            conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE} ADD COLUMN IF NOT EXISTS year INTEGER;"))
            conn.commit()
        except: pass

def get_shapefile_crs(path):
    try:
        mini_gdf = gpd.read_file(path, rows=1)
        return mini_gdf.crs
    except Exception as e:
        print(f"❌ Error detectando CRS: {e}")
        return None

def load_census_layer():
    engine = create_engine(DB_URL)
    census_year = extract_year_from_filename(CSV_FILENAME)
    
    print(f"🚀 INICIANDO CENSUS FINAL (Fix Tuple) - Año: {census_year}")
    
    ensure_table_structure(engine)

    # 1. LEER CSV
    print("📥 [1/4] Leyendo CSV INE...")
    try:
        preview = pd.read_csv(RENTA_CSV, sep=';', dtype=str, encoding='utf-8', nrows=5)
        col_ind = next((c for c in preview.columns if 'Indicadores' in c), None)
        if not col_ind: return

        cols_to_use = ['Secciones', 'Periodo', 'Total', col_ind]
        df_csv = pd.read_csv(RENTA_CSV, sep=';', dtype=str, encoding='utf-8', usecols=lambda c: c in cols_to_use)
        
        df_csv = df_csv[df_csv[col_ind].str.contains(TARGET_INDICATOR, case=False, na=False)]
        
        if 'Periodo' in df_csv.columns:
            data_year = pd.to_numeric(df_csv['Periodo'], errors='coerce').max()
            df_csv = df_csv[pd.to_numeric(df_csv['Periodo'], errors='coerce') == data_year]

        df_csv = df_csv.dropna(subset=['Secciones'])
        
        # ID CLEANING
        df_csv['CUSEC'] = df_csv['Secciones'].astype(str).str.strip().str[:10]
        df_csv['renta_hogar'] = df_csv['Total'].apply(clean_currency_ine)
        
        df_renta_clean = df_csv[['CUSEC', 'renta_hogar']].copy()
        del df_csv
        gc.collect()
        
        print(f"   📊 Datos cargados: {len(df_renta_clean)} registros.")

    except Exception as e:
        print(f"❌ Error CSV: {e}")
        return

    # 2. DETECTAR CRS MAPA
    map_crs = get_shapefile_crs(CENSUS_SHP)
    if not map_crs: return
    print(f"   🗺️ CRS Mapa: {map_crs}")

    # 3. CIUDADES
    target_cities = ACTIVE_CITIES
    if not target_cities:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT city FROM core.hexagons"))
            target_cities = [r[0] for r in res]

    # 4. PROCESO POR CIUDAD
    for city in target_cities:
        print(f"\n📍 Procesando: {city}")
        
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {SCHEMA}.{TABLE} WHERE city = :city AND year = :year"), 
                         {"city": city, "year": census_year})
            conn.commit()

        # A. Obtener Grid
        try:
            sql_grid = text("SELECT h3_id, geometry FROM core.hexagons WHERE city = :city")
            gdf_hex_city = gpd.read_postgis(sql_grid, engine, params={"city": city}, geom_col="geometry")
            if gdf_hex_city.empty:
                print("      ⚠️ Sin hexágonos. Saltando.")
                continue

            # B. Transformar Caja (BBOX)
            minx, miny, maxx, maxy = gdf_hex_city.total_bounds
            bbox_polygon_4326 = box(minx, miny, maxx, maxy)
            
            bbox_series = gpd.GeoSeries([bbox_polygon_4326], crs="EPSG:4326")
            bbox_series_transformed = bbox_series.to_crs(map_crs)
            bbox_transformed = bbox_series_transformed.total_bounds # Esto devuelve un numpy array
            
        except Exception as e:
            print(f"      ❌ Error grid: {e}")
            continue

        # C. Leer Mapa (FIX APLICADO AQUÍ: tuple())
        print("      🗺️ Leyendo mapa...")
        try:
            # ⬇️⬇️⬇️ AQUÍ ESTÁ EL ARREGLO: tuple(...) ⬇️⬇️⬇️
            gdf_mapa_local = gpd.read_file(CENSUS_SHP, bbox=tuple(bbox_transformed))
            
            if gdf_mapa_local.empty:
                print(f"      ⚠️ Mapa vacío para esta zona.")
                continue

            gdf_mapa_local = gdf_mapa_local.to_crs("EPSG:4326")
            col_mapa = next((c for c in gdf_mapa_local.columns if c in ['CUSEC', 'CD_SECC']), None)
            if col_mapa:
                gdf_mapa_local.rename(columns={col_mapa: 'CUSEC'}, inplace=True)
                gdf_mapa_local['CUSEC'] = gdf_mapa_local['CUSEC'].astype(str).str.strip()
            else:
                print("      ❌ Sin columna CUSEC.")
                continue

        except Exception as e:
            print(f"      ❌ Error leyendo Shapefile: {e}")
            continue

        # D. Cruce
        gdf_census_local = gdf_mapa_local.merge(df_renta_clean, on='CUSEC', how='inner')
        if gdf_census_local.empty:
            print("      ⚠️ Cruce vacío (IDs no coinciden).")
            continue

        # E. Interpolación
        gdf_census_local = gdf_census_local.to_crs("EPSG:3857")
        gdf_hex_proj = gdf_hex_city.to_crs("EPSG:3857")
        gdf_census_local['area_orig'] = gdf_census_local.area
        
        try:
            overlay = gpd.overlay(gdf_census_local, gdf_hex_proj, how='intersection')
            overlay['weight'] = overlay.area / overlay['area_orig']
            overlay['w_renta'] = overlay['renta_hogar'] * overlay['weight']
            
            result = overlay.groupby('h3_id')['w_renta'].sum().reset_index()
            result.rename(columns={'w_renta': 'renta_media_hogar'}, inplace=True)
            
            result['city'] = city
            result['year'] = census_year
            
            result.to_sql(TABLE, engine, schema=SCHEMA, if_exists='append', index=False)
            print(f"      ✅ Guardados {len(result)} registros.")
            
        except Exception as e:
            print(f"      ❌ Error cálculo: {e}")

        del gdf_mapa_local, gdf_hex_city, gdf_census_local, result
        gc.collect()

    print("\n🏁 Finalizando índices...")
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_census_h3 ON {SCHEMA}.{TABLE} (h3_id);"))
        conn.commit()
    print("✅ PROCESO COMPLETADO.")

if __name__ == "__main__":
    load_census_layer()
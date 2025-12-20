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
# Usamos el CRS del script original para cálculos de área precisos
CALC_CRS = "EPSG:25830" 

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
        conn.commit()

def get_shapefile_crs(path):
    try:
        mini = gpd.read_file(path, rows=1)
        return mini.crs
    except: return None

def load_census_layer():
    engine = create_engine(DB_URL)
    census_year = extract_year_from_filename(CSV_FILENAME)
    print(f"🚀 INICIANDO CENSUS (Lógica de Pesos Corregida) - Año: {census_year}")
    
    ensure_table_structure(engine)

    # 1. LEER CSV (Lógica robusta del script original)
    print("📥 [1/4] Leyendo CSV INE...")
    try:
        df = pd.read_csv(RENTA_CSV, sep=';', dtype=str, encoding='utf-8')
        df.columns = df.columns.str.strip()
        
        # Filtro Indicador y Año
        col_ind = [c for c in df.columns if 'Indicadores' in c][0]
        mask_ind = df[col_ind].str.contains(TARGET_INDICATOR, case=False, na=False)
        
        periodos = pd.to_numeric(df['Periodo'], errors='coerce')
        target_year = periodos.max()
        mask_per = periodos == target_year
        
        df_clean = df[mask_ind & mask_per].copy()
        
        df_clean['renta'] = df_clean['Total'].apply(clean_currency_ine)
        df_clean['CUSEC'] = df_clean['Secciones'].astype(str).str.strip().str[:10]
        
        # Solo rentas válidas
        df_renta = df_clean[df_clean['renta'] > 0][['CUSEC', 'renta']].copy()
        
        del df, df_clean
        gc.collect()
        print(f"   📊 Datos CSV listos: {len(df_renta)} secciones.")

    except Exception as e:
        print(f"❌ Error CSV: {e}")
        return

    # 2. CRS MAPA
    map_crs = get_shapefile_crs(CENSUS_SHP)
    if not map_crs: return

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
            sql = text("SELECT h3_id, geometry FROM core.hexagons WHERE city = :city")
            gdf_hex = gpd.read_postgis(sql, engine, params={"city": city}, geom_col="geometry")
            if gdf_hex.empty: continue

            # Caja para recorte
            bounds_4326 = gdf_hex.total_bounds
            bbox_poly = box(*bounds_4326)
            bbox_trans = gpd.GeoSeries([bbox_poly], crs="EPSG:4326").to_crs(map_crs).total_bounds
        except Exception as e:
            print(f"      ❌ Error grid: {e}")
            continue

        # B. Leer Mapa Local
        try:
            gdf_mapa = gpd.read_file(CENSUS_SHP, bbox=tuple(bbox_trans)).to_crs("EPSG:4326")
            if gdf_mapa.empty: continue

            col_mapa = next((c for c in gdf_mapa.columns if c in ['CUSEC', 'CD_SECC']), None)
            gdf_mapa.rename(columns={col_mapa: 'CUSEC'}, inplace=True)
            gdf_mapa['CUSEC'] = gdf_mapa['CUSEC'].astype(str).str.strip()
        except Exception as e:
            print(f"      ❌ Error mapa: {e}")
            continue

        # C. Cruce
        gdf_merged = gdf_mapa.merge(df_renta, on='CUSEC', how='inner')
        if gdf_merged.empty: continue

        # D. INTERPOLACIÓN (CORRECCIÓN DE LÓGICA)
        # ---------------------------------------------------------------------
        try:
            # Proyectamos ambos al sistema de cálculo (Metros)
            gdf_merged = gdf_merged.to_crs(CALC_CRS)
            gdf_hex_proj = gdf_hex.to_crs(CALC_CRS)
            
            # 1. Calculamos el área total del hexágono (denominador correcto para variables intensivas)
            gdf_hex_proj['area_hex'] = gdf_hex_proj.geometry.area
            
            # 2. Intersección
            overlay = gpd.overlay(gdf_merged, gdf_hex_proj, how='intersection')
            
            # 3. PESO = área_intersección / área_del_hexágono
            overlay['weight'] = overlay.geometry.area / overlay['area_hex']
            
            # 4. Valor ponderado
            overlay['w_renta'] = overlay['renta'] * overlay['weight']
            
            # 5. Agrupar
            result = overlay.groupby('h3_id')['w_renta'].sum().reset_index()
            result.rename(columns={'w_renta': 'renta_media_hogar'}, inplace=True)
            
            result['city'] = city
            result['year'] = census_year
            
            result.to_sql(TABLE, engine, schema=SCHEMA, if_exists='append', index=False)
            print(f"      ✅ Guardados {len(result)} registros. Max Renta: {result['renta_media_hogar'].max():.0f}€")

        except Exception as e:
            print(f"      ❌ Error cálculo: {e}")

        del gdf_mapa, gdf_hex, gdf_merged, result
        gc.collect()

    print("\n✅ PROCESO COMPLETADO.")

if __name__ == "__main__":
    load_census_layer()
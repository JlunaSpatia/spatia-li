import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import os
import sys
import warnings
import glob 

# ==========================================
# 1. SETUP DE RUTAS (CORREGIDO PARA CARPETA 'etl')
# ==========================================
# Ubicación actual: /home/jesus/spatia-li/etl/03_enrich_Demo.py
current_dir = os.path.dirname(os.path.abspath(__file__))

# CORRECCIÓN: Subimos SOLO 1 NIVEL para llegar a /spatia-li
project_root = os.path.dirname(current_dir) 

sys.path.append(project_root)

# Importamos config
try:
    from config import DB_CONNECTION_STR, DATA_DIR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"
    DATA_DIR = "data/raw"

# Rutas absolutas
RAW_PATH = os.path.join(project_root, DATA_DIR)
SHP_PATH = os.path.join(RAW_PATH, "SECC_CE_20230101.shp")

warnings.filterwarnings("ignore")

# ==========================================
# 2. FUNCIONES DE LIMPIEZA
# ==========================================

def clean_ine_csv(path):
    print(f"   🧹 Procesando archivo Maestro INE: {os.path.basename(path)}...")
    
    if not os.path.exists(path):
        print(f"      ❌ Error: No encuentro el archivo en {path}")
        return pd.DataFrame()

    try:
        # 1. LECTURA
        df = pd.read_csv(path, sep=';', encoding='utf-8', dtype=str)
        df.columns = df.columns.str.strip() 
        
        # 2. FILTROS BÁSICOS
        if 'Secciones' not in df.columns:
            print("      ❌ Error: Columna 'Secciones' no encontrada.")
            return pd.DataFrame()

        df = df[df['Secciones'].notna()]
        
        # Filtro Indicador
        target_indicator = "Renta bruta media por hogar"
        col_indicador = [c for c in df.columns if 'Indicadores' in c]
        
        if not col_indicador:
             print("      ❌ Error: No encuentro columna de Indicadores (revisa el CSV).")
             return pd.DataFrame()
             
        mask_indicador = df[col_indicador[0]].str.contains(target_indicator, case=False, na=False)
        
        # Filtro Periodo (Automático: coge el año más alto del CSV)
        periodos = pd.to_numeric(df['Periodo'], errors='coerce')
        target_year = periodos.max()
        mask_periodo = periodos == target_year
        
        df_clean = df[mask_indicador & mask_periodo].copy()
        
        print(f"      -> Datos del año {int(target_year)}: {len(df_clean)} secciones encontradas.")

        if df_clean.empty: return pd.DataFrame()

        # 3. LIMPIEZA VALORES
        def clean_currency(x):
            if pd.isna(x): return 0.0
            clean_str = str(x).replace('.', '').replace(',', '.')
            try: return float(clean_str)
            except: return 0.0

        df_clean['renta'] = df_clean['Total'].apply(clean_currency)
        df_clean['CUSEC'] = df_clean['Secciones'].astype(str).str.strip().str[:10]
        
        return df_clean[df_clean['renta'] > 0][['CUSEC', 'renta']]

    except Exception as e:
        print(f"      ❌ Error leyendo CSV: {e}")
        return pd.DataFrame()

def spatial_interpolation(hex_gdf, census_gdf, value_col):
    print("   ✂️ Cruzando geometrías (Interpolación Areal)...")
    
    target_crs = "EPSG:25830"
    
    if hex_gdf.crs is None: hex_gdf.set_crs("EPSG:4326", inplace=True)
    if census_gdf.crs is None: census_gdf.set_crs("EPSG:4326", inplace=True)

    hex_gdf = hex_gdf.to_crs(target_crs)
    census_gdf = census_gdf.to_crs(target_crs)
    
    hex_gdf['area_hex'] = hex_gdf.geometry.area
    
    overlay = gpd.overlay(hex_gdf, census_gdf, how='intersection')
    
    overlay['weight'] = overlay.geometry.area / overlay['area_hex']
    overlay['weighted_val'] = overlay[value_col] * overlay['weight']
    
    print("   ∑ Agrupando resultados...")
    result = overlay.groupby('h3_index')['weighted_val'].sum().reset_index()
    result.rename(columns={'weighted_val': 'avg_income'}, inplace=True)
    
    return result

# ==========================================
# 3. PROCESO PRINCIPAL
# ==========================================
def enrich_demographics():
    print("💶 PASO 03: INTEGRACIÓN DE RENTA (INE MAESTRO)...")
    engine = create_engine(DB_CONNECTION_STR)
    
    # 1. BUSCAR CSV MAESTRO (Dinámico)
    # El patrón INE_*_Renta.csv encontrará INE_2023_Renta.csv hoy
    # y INE_2024_Renta.csv el año que viene.
    search_pattern = os.path.join(RAW_PATH, "INE_*_Renta.csv")
    found_files = glob.glob(search_pattern)
    
    if not found_files:
        print(f"❌ No encuentro ningún archivo 'INE_xxxx_Renta.csv' en {RAW_PATH}")
        sys.exit(1)
    
    # Ordenamos: 2024 va después de 2023, así que cogemos el último.
    found_files.sort()
    target_csv = found_files[-1]
    
    # 2. CARGAR Y LIMPIAR
    df_renta_total = clean_ine_csv(target_csv)
    
    if df_renta_total.empty:
        print("❌ El CSV del INE no devolvió datos válidos.")
        sys.exit(1)

    # 3. CARGAR MAPA
    if not os.path.exists(SHP_PATH):
        print(f"❌ ERROR CRÍTICO: No encuentro el mapa censal en {SHP_PATH}")
        sys.exit(1)
        
    print("   🗺️ Leyendo mapa censal (Shapefile)...")
    gdf_mapa = gpd.read_file(SHP_PATH)
    gdf_mapa['CUSEC'] = gdf_mapa['CUSEC'].astype(str).str.strip()
    
    # 4. JOIN
    print("   🔗 Uniendo Tabla Renta + Mapa Geográfico...")
    gdf_census_full = gdf_mapa.merge(df_renta_total, on='CUSEC', how='inner')
    print(f"      -> Secciones con renta mapeada: {len(gdf_census_full)}")
    
    if len(gdf_census_full) == 0:
        print("⚠️ ALERTA: El cruce ha dado 0 resultados.")
        sys.exit(1)

    # 5. CARGAR HEXÁGONOS
    print("   ⬡ Leyendo hexágonos de PostGIS...")
    try:
        sql = "SELECT h3_index, geometry FROM retail_hexagons"
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    except Exception as e:
        print(f"❌ Error leyendo PostGIS: {e}")
        sys.exit(1)
    
    if gdf_hex.empty:
        print("⚠️ No hay hexágonos en la base de datos.")
        sys.exit(0)

    # 6. INTERPOLACIÓN
    df_income_final = spatial_interpolation(gdf_hex, gdf_census_full, 'renta')
    
    # 7. GUARDAR
    print("💾 Guardando Renta en BBDD...")
    
    df_income_final.to_sql('temp_income', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS retail_hexagons_enriched AS SELECT * FROM retail_hexagons WHERE 1=0;"))
        
        print("      -> Sincronizando estructura de tabla...")
        conn.execute(text("""
            INSERT INTO retail_hexagons_enriched (h3_index, geometry, city, lat, lon, dist_cafe, dist_gym, dist_shop, dist_transit)
            SELECT h.h3_index, h.geometry, h.city, h.lat, h.lon, h.dist_cafe, h.dist_gym, h.dist_shop, h.dist_transit
            FROM retail_hexagons h
            WHERE NOT EXISTS (
                SELECT 1 FROM retail_hexagons_enriched e WHERE e.h3_index = h.h3_index
            );
        """))

        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS avg_income FLOAT;"))
        
        print("      -> Actualizando valores de renta...")
        conn.execute(text("""
            UPDATE retail_hexagons_enriched m
            SET avg_income = t.avg_income
            FROM temp_income t
            WHERE m.h3_index = t.h3_index;
        """))
        
        conn.execute(text("DROP TABLE temp_income;"))
        
    print("✅ ¡RENTA INTEGRADA CORRECTAMENTE!")
    
    top_zona = df_income_final.sort_values('avg_income', ascending=False).iloc[0]
    print(f"Éxito. Renta integrada usando {os.path.basename(target_csv)}. Máx: {top_zona['avg_income']:.0f}€")

if __name__ == "__main__":
    enrich_demographics()
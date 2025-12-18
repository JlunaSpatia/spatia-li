import sys
import os

# 1. Configuración de rutas
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(BASE_DIR)

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_URL

# --- CONFIGURACIÓN ---
SHP_FILENAME = "SECC_CE_20230101.shp"
CSV_FILENAME = "INE_2023_Renta.csv"
TARGET_INDICATOR = "Renta bruta media por hogar" 

CENSUS_SHP = os.path.join(BASE_DIR, "data", "raw", SHP_FILENAME)
RENTA_CSV = os.path.join(BASE_DIR, "data", "raw", CSV_FILENAME)

def clean_currency_ine(x):
    """Limpieza robusta de números formato ES"""
    if pd.isna(x): return 0.0
    clean_str = str(x).replace('.', '').replace(',', '.')
    try:
        return float(clean_str)
    except:
        return 0.0

def load_census_layer():
    engine = create_engine(DB_URL)
    print(f"🚀 INICIANDO CENSUS CORE (SOLO VARIABLE: '{TARGET_INDICATOR}')...")

    # 1. LEER GRID
    print("📥 [1/5] Leyendo Grid H3...")
    try:
        gdf_grid = gpd.read_postgis("SELECT h3_id, geometry FROM core.hexagons", con=engine, geom_col="geometry")
        if gdf_grid.empty: return
    except Exception as e:
        print(f"❌ Error BBDD: {e}")
        return

    # 2. LEER MAPA + CLIPPING
    print(f"📥 [2/5] Leyendo Mapa y Recortando...")
    try:
        gdf_mapa_full = gpd.read_file(CENSUS_SHP).to_crs("EPSG:4326")
        minx, miny, maxx, maxy = gdf_grid.total_bounds
        gdf_mapa = gdf_mapa_full.cx[minx:maxx, miny:maxy].copy()
        del gdf_mapa_full
        
        # Normalizar ID Mapa a CUSEC
        col_mapa = next((c for c in gdf_mapa.columns if c in ['CUSEC', 'CD_SECC']), None)
        if col_mapa:
            gdf_mapa.rename(columns={col_mapa: 'CUSEC'}, inplace=True)
            gdf_mapa['CUSEC'] = gdf_mapa['CUSEC'].astype(str).str.strip()
        else:
            print("❌ No hay columna CUSEC en el Shapefile.")
            return
            
    except Exception as e:
        print(f"❌ Error Shapefile: {e}")
        return

    # 3. LEER CSV Y FILTRAR
    print(f"📥 [3/5] Procesando CSV...")
    try:
        df = pd.read_csv(RENTA_CSV, sep=';', dtype=str, encoding='utf-8')
        
        # Buscamos indicador
        col_ind = next((c for c in df.columns if 'Indicadores' in c), None)
        if not col_ind: return
            
        df = df[df[col_ind].str.contains(TARGET_INDICATOR, case=False, na=False)].copy()
        
        # Filtro Año
        if 'Periodo' in df.columns:
            target_year = pd.to_numeric(df['Periodo'], errors='coerce').max()
            df = df[pd.to_numeric(df['Periodo'], errors='coerce') == target_year]
        
        # Filtro Secciones validas
        df = df.dropna(subset=['Secciones'])
        
        # --- CORRECCIÓN CRÍTICA DE ID ---
        # Usamos la lógica de TU script antiguo: 
        # Coger directamente la columna 'Secciones' y recortar a 10 caracteres.
        print("   🔨 Extrayendo IDs (Lógica original)...")
        
        df['CUSEC'] = df['Secciones'].astype(str).str.strip().str[:10]
        
        # Limpiar Valor
        df['renta_hogar'] = df['Total'].apply(clean_currency_ine)
        
        # Tabla final limpia
        df_final = df[['CUSEC', 'renta_hogar']].copy()
        
        # Filtro final cruce
        valid_ids = set(gdf_mapa['CUSEC'])
        df_renta = df_final[df_final['CUSEC'].isin(valid_ids)].copy()
        
        print(f"   ✅ Cruce exitoso: {len(df_renta)} secciones coincidentes.")
        
        if len(df_renta) == 0:
            print(f"❌ ERROR: 0 cruces.")
            print(f"   ID Mapa Ejemplo: '{list(valid_ids)[0]}'")
            print(f"   ID CSV Ejemplo:  '{df_final['CUSEC'].iloc[0]}'")
            return

    except Exception as e:
        print(f"❌ Error CSV: {e}")
        return

    # 4. INTERPOLACIÓN
    print("🔄 [4/5] Interpolando Renta hacia Hexágonos...")
    
    gdf_merged = gdf_mapa.merge(df_renta, on='CUSEC', how='left').fillna(0)
    gdf_merged = gdf_merged.to_crs("EPSG:3857")
    gdf_grid_proj = gdf_grid.to_crs("EPSG:3857")
    gdf_merged['area_orig'] = gdf_merged.area
    
    # Intersección
    overlay = gpd.overlay(gdf_merged, gdf_grid_proj, how='intersection')
    
    # Peso
    overlay['weight'] = overlay.area / overlay['area_orig']
    overlay['w_renta'] = overlay['renta_hogar'] * overlay['weight']
    
    # Agrupar
    result = overlay.groupby('h3_id')['w_renta'].sum().reset_index()
    result.rename(columns={'w_renta': 'renta_media_hogar'}, inplace=True)

    # 5. GUARDAR
    print(f"💾 [5/5] Guardando en BBDD 'core.census'...")
    try:
        result.to_sql("census", engine, schema="core", if_exists="replace", index=False)
        with engine.connect() as con:
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_census_h3 ON core.census (h3_id);"))
            con.commit()
        
        max_r = result['renta_media_hogar'].max()
        print(f"✅ ÉXITO. Renta Máxima: {max_r:,.0f}€")
        
    except Exception as e:
        print(f"❌ Error SQL: {e}")

if __name__ == "__main__":
    load_census_layer()
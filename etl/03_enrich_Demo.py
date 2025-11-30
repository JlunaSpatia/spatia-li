import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import os
import warnings
import numpy as np

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# Rutas de tus archivos
SHP_PATH = "data/raw/SECC_CE_20230101.shp"
CSV_MADRID = "data/raw/ADHR_Madrid.csv"
CSV_VALENCIA = "data/raw/ADHR_Valencia.csv"

warnings.filterwarnings("ignore")

def clean_ine_csv(path):
    print(f"   🧹 Procesando archivo: {path}...")
    
    if not os.path.exists(path):
        print(f"      ❌ ERROR: No encuentro el archivo {path}")
        return pd.DataFrame()

    try:
        # 1. LECTURA (Separador ;)
        df = pd.read_csv(path, sep=';', encoding='utf-8', dtype=str)
        df.columns = df.columns.str.strip() # Limpiar nombres de columnas
        
        # 2. FILTROS ESTRICTOS (Tu corrección)
        # Eliminamos filas sin sección (totales municipales)
        df = df[df['Secciones'].notna()]
        
        # A. FILTRO INDICADOR: "Renta bruta media por hogar"
        # Usamos contains para evitar problemas con espacios extra, pero buscamos la frase exacta
        target_indicator = "Renta bruta media por hogar"
        mask_indicador = df['Indicadores de renta media y mediana'].str.contains(target_indicator, case=False, na=False)
        
        # B. FILTRO PERIODO: Año más reciente (Debería ser 2023 si está en el archivo)
        periodos = pd.to_numeric(df['Periodo'], errors='coerce')
        target_year = periodos.max()
        mask_periodo = periodos == target_year
        
        # Aplicamos filtros
        df_clean = df[mask_indicador & mask_periodo].copy()
        
        print(f"      -> Filtros: '{target_indicator}' | Año {int(target_year)}")
        print(f"      -> Datos encontrados: {len(df_clean)} secciones")

        if df_clean.empty:
            print("      ⚠️ AVISO: No se han encontrado datos con esos filtros. Revisa el nombre del indicador en el CSV.")
            # Debug: Mostrar qué indicadores existen
            print(f"      Indicadores disponibles: {df['Indicadores de renta media y mediana'].unique()[:3]}")
            return pd.DataFrame()

        # 3. LIMPIEZA DE VALORES (Columna 'Total')
        # Formato "34.500" -> 34500.0
        def clean_currency(x):
            if pd.isna(x): return 0.0
            clean_str = str(x).replace('.', '').replace(',', '.')
            try:
                return float(clean_str)
            except:
                return 0.0

        df_clean['renta'] = df_clean['Total'].apply(clean_currency)
        
        # 4. LIMPIEZA DE CÓDIGO (CUSEC - Primeros 10 dígitos)
        # Ejemplo: "2800201001 Sección 1" -> "2800201001"
        df_clean['CUSEC'] = df_clean['Secciones'].astype(str).str[:10]
        
        # Eliminar rentas 0
        df_final = df_clean[df_clean['renta'] > 0][['CUSEC', 'renta']]
        
        return df_final

    except Exception as e:
        print(f"      ❌ Error leyendo CSV: {e}")
        return pd.DataFrame()

def spatial_interpolation(hex_gdf, census_gdf, value_col):
    print("   ✂️  Cruzando geometrías (Interpolación Areal)...")
    
    # Proyectar a UTM 30N (Metros)
    hex_gdf = hex_gdf.to_crs(epsg=25830)
    census_gdf = census_gdf.to_crs(epsg=25830)
    
    hex_gdf['area_hex'] = hex_gdf.geometry.area
    
    # Intersección
    overlay = gpd.overlay(hex_gdf, census_gdf, how='intersection')
    
    # Peso ponderado
    overlay['weight'] = overlay.geometry.area / overlay['area_hex']
    overlay['weighted_val'] = overlay[value_col] * overlay['weight']
    
    # Agrupar
    print("   ∑  Agrupando resultados...")
    result = overlay.groupby('h3_index')['weighted_val'].sum().reset_index()
    result.rename(columns={'weighted_val': 'avg_income'}, inplace=True)
    
    return result

def enrich_demographics():
    print("💶 PASO 03: INTEGRACIÓN DE RENTA (INE)...")
    engine = create_engine(DB_URL)
    
    # 1. CARGAR DATOS
    df_mad = clean_ine_csv(CSV_MADRID)
    df_val = clean_ine_csv(CSV_VALENCIA)
    
    if df_mad.empty and df_val.empty:
        print("❌ ERROR: No hay datos válidos. Abortando.")
        return

    df_renta_total = pd.concat([df_mad, df_val])
    
    # 2. CARGAR MAPA
    if not os.path.exists(SHP_PATH):
        print(f"❌ ERROR: Falta mapa {SHP_PATH}")
        return
        
    print("   🗺️  Leyendo mapa censal...")
    gdf_mapa = gpd.read_file(SHP_PATH)
    gdf_mapa['CUSEC'] = gdf_mapa['CUSEC'].astype(str).str.strip()
    
    # 3. JOIN
    print("   🔗 Uniendo Datos + Mapa...")
    gdf_census_full = gdf_mapa.merge(df_renta_total, on='CUSEC', how='inner')
    print(f"      -> Secciones listas para cruce: {len(gdf_census_full)}")
    
    if len(gdf_census_full) == 0:
        print("⚠️ ERROR DE CRUCE: 0 coincidencias. Revisa los códigos CUSEC.")
        print(f"   Ejemplo Mapa: '{gdf_mapa['CUSEC'].iloc[0]}'")
        print(f"   Ejemplo CSV:  '{df_renta_total['CUSEC'].iloc[0]}'")
        return

    # 4. CARGAR HEXÁGONOS
    print("   ⬡  Leyendo hexágonos de PostGIS...")
    sql = "SELECT h3_index, geometry FROM retail_hexagons"
    gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    # 5. INTERPOLACIÓN
    df_income_final = spatial_interpolation(gdf_hex, gdf_census_full, 'renta')
    
    # 6. GUARDAR (UPDATE)
    print("💾 Guardando Renta en BBDD...")
    df_income_final.to_sql('temp_income', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS avg_income FLOAT;"))
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET avg_income = s.avg_income
            FROM temp_income AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_income;"))
        conn.commit()
        
    print("✅ ¡RENTA BRUTA POR HOGAR INTEGRADA!")
    print(df_income_final.sort_values('avg_income', ascending=False).head(3))

if __name__ == "__main__":
    enrich_demographics()
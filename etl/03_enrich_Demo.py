import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import os
import sys
import warnings
import numpy as np

# --- CONFIGURACIÓN ---
# Importamos variables del config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

# Definimos rutas absolutas para robustez
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # ~/spatia-li/
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")

SHP_PATH = os.path.join(DATA_RAW, "SECC_CE_20230101.shp")
# Nota: Si tienes un solo CSV maestro o varios, ajusta aquí.
# Asumo que pueden existir o no, así que los listo.
POSSIBLE_CSVS = [
    os.path.join(DATA_RAW, "ADHR_Madrid.csv"),
    os.path.join(DATA_RAW, "ADHR_Valencia.csv")
]

warnings.filterwarnings("ignore")

def clean_ine_csv(path):
    print(f"   🧹 Procesando archivo: {os.path.basename(path)}...")
    
    if not os.path.exists(path):
        # No es error crítico, quizás solo estamos procesando Madrid y no tenemos Valencia aún
        print(f"      ℹ️ Info: No encuentro {os.path.basename(path)}, saltando.")
        return pd.DataFrame()

    try:
        # 1. LECTURA
        df = pd.read_csv(path, sep=';', encoding='utf-8', dtype=str)
        df.columns = df.columns.str.strip() 
        
        # 2. FILTROS
        if 'Secciones' not in df.columns:
            # A veces el INE cambia cabeceras. Chequeo básico.
            print("      ❌ Error: Columna 'Secciones' no encontrada.")
            return pd.DataFrame()

        df = df[df['Secciones'].notna()]
        
        # Filtro Indicador
        target_indicator = "Renta bruta media por hogar"
        mask_indicador = df['Indicadores de renta media y mediana'].str.contains(target_indicator, case=False, na=False)
        
        # Filtro Periodo (Max Year)
        periodos = pd.to_numeric(df['Periodo'], errors='coerce')
        target_year = periodos.max()
        mask_periodo = periodos == target_year
        
        df_clean = df[mask_indicador & mask_periodo].copy()
        
        print(f"      -> Datos del año {int(target_year)}: {len(df_clean)} secciones")

        if df_clean.empty: return pd.DataFrame()

        # 3. LIMPIEZA VALORES
        def clean_currency(x):
            if pd.isna(x): return 0.0
            clean_str = str(x).replace('.', '').replace(',', '.')
            try: return float(clean_str)
            except: return 0.0

        df_clean['renta'] = df_clean['Total'].apply(clean_currency)
        
        # 4. LIMPIEZA CÓDIGO (CUSEC)
        # El INE a veces pone el código completo, cogemos los primeros 10 chars que son el CUSEC estándar
        df_clean['CUSEC'] = df_clean['Secciones'].astype(str).str.strip().str[:10]
        
        # Filtrar rentas válidas
        return df_clean[df_clean['renta'] > 0][['CUSEC', 'renta']]

    except Exception as e:
        print(f"      ❌ Error leyendo CSV: {e}")
        return pd.DataFrame()

def spatial_interpolation(hex_gdf, census_gdf, value_col):
    print("   ✂️ Cruzando geometrías (Interpolación Areal)...")
    
    # Proyección a Metros (UTM 30N - España Peninsular)
    # Es vital para calcular áreas correctamente
    target_crs = "EPSG:25830"
    hex_gdf = hex_gdf.to_crs(target_crs)
    census_gdf = census_gdf.to_crs(target_crs)
    
    # Area original del hexágono (para calcular % de solape)
    hex_gdf['area_hex'] = hex_gdf.geometry.area
    
    # Intersección (Hexágono "recortado" por la sección censal)
    overlay = gpd.overlay(hex_gdf, census_gdf, how='intersection')
    
    # Cálculo del peso: ¿Qué % del hexágono ocupa este trozo de sección?
    overlay['weight'] = overlay.geometry.area / overlay['area_hex']
    
    # Valor ponderado: Si el hexágono toca un 10% de un barrio rico, coge el 10% de esa renta
    # NOTA: Para Renta Media, esto es una aproximación. Lo ideal es Population Weighted, 
    # pero Areal Weighted es el estándar de industria para MVP.
    overlay['weighted_val'] = overlay[value_col] * overlay['weight']
    
    print("   ∑ Agrupando resultados...")
    # Sumamos los trozos para reconstruir el valor del hexágono completo
    # OJO: Al ser Renta MEDIA, la suma ponderada funciona si asumimos distribución homogénea
    result = overlay.groupby('h3_index')['weighted_val'].sum().reset_index()
    result.rename(columns={'weighted_val': 'avg_income'}, inplace=True)
    
    return result

def enrich_demographics():
    print("💶 PASO 03: INTEGRACIÓN DE RENTA (INE)...")
    engine = create_engine(DB_CONNECTION_STR)
    
    # 1. CARGAR DATOS INE
    dfs = []
    for csv_path in POSSIBLE_CSVS:
        df_temp = clean_ine_csv(csv_path)
        if not df_temp.empty:
            dfs.append(df_temp)
    
    if not dfs:
        print("❌ ERROR: No se cargó ningún dato de renta válido.")
        return

    df_renta_total = pd.concat(dfs)
    
    # 2. CARGAR MAPA SHAPEFILE
    if not os.path.exists(SHP_PATH):
        print(f"❌ ERROR CRÍTICO: No encuentro el mapa censal en {SHP_PATH}")
        return
        
    print("   🗺️ Leyendo mapa censal (Shapefile)...")
    gdf_mapa = gpd.read_file(SHP_PATH)
    # Asegurar que CUSEC sea string limpio para el join
    gdf_mapa['CUSEC'] = gdf_mapa['CUSEC'].astype(str).str.strip()
    
    # 3. JOIN (Dato + Mapa)
    print("   🔗 Uniendo Tabla Renta + Mapa Geográfico...")
    gdf_census_full = gdf_mapa.merge(df_renta_total, on='CUSEC', how='inner')
    print(f"      -> Secciones con renta mapeada: {len(gdf_census_full)}")
    
    if len(gdf_census_full) == 0:
        print("⚠️ ALERTA: El cruce ha dado 0 resultados. Revisa que los códigos CUSEC coincidan.")
        return

    # 4. CARGAR HEXÁGONOS (Desde PostGIS)
    print("   ⬡ Leyendo hexágonos de PostGIS...")
    try:
        sql = "SELECT h3_index, geometry FROM retail_hexagons"
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    except Exception as e:
        print(f"❌ Error leyendo PostGIS: {e}")
        return
    
    # 5. INTERPOLACIÓN
    df_income_final = spatial_interpolation(gdf_hex, gdf_census_full, 'renta')
    
    # 6. GUARDAR Y SINCRONIZAR
    print("💾 Guardando Renta en BBDD...")
    
    df_income_final.to_sql('temp_income', engine, if_exists='replace', index=False)
    
    # Usamos transaction block para seguridad
    with engine.begin() as conn:
        # A. Crear Master Table si no existe
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS retail_hexagons_enriched AS 
            SELECT * FROM retail_hexagons WHERE 1=0;
        """))
        # (El WHERE 1=0 crea la estructura vacía si no existe, luego rellenamos)
        
        # B. Sincronizar Nuevos Hexágonos (Insertar los que falten desde la base)
        print("      -> Sincronizando nuevos territorios...")
        conn.execute(text("""
            INSERT INTO retail_hexagons_enriched (h3_index, geometry, city, lat, lon, dist_cafe, dist_gym, dist_shop, dist_transit)
            SELECT h.h3_index, h.geometry, h.city, h.lat, h.lon, h.dist_cafe, h.dist_gym, h.dist_shop, h.dist_transit
            FROM retail_hexagons h
            WHERE NOT EXISTS (
                SELECT 1 FROM retail_hexagons_enriched e WHERE e.h3_index = h.h3_index
            );
        """))

        # C. Añadir columna Renta
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS avg_income FLOAT;"))
        
        # D. Update Masivo
        print("      -> Actualizando valores de renta...")
        conn.execute(text("""
            UPDATE retail_hexagons_enriched m
            SET avg_income = t.avg_income
            FROM temp_income t
            WHERE m.h3_index = t.h3_index;
        """))
        
        # E. Limpieza
        conn.execute(text("DROP TABLE temp_income;"))
        
    print("✅ ¡RENTA INTEGRADA CORRECTAMENTE!")
    print(df_income_final.sort_values('avg_income', ascending=False).head(3))

if __name__ == "__main__":
    enrich_demographics()
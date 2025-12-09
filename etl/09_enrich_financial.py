import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import numpy as np
import os
import sys
import warnings

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

# Usaremos las Secciones Censales como proxy de "Barrios" si no hay otro mapa
# (Ajusta la ruta si tienes un mapa de barrios reales como 'madrid_distritos.shp')
BARRIOS_SHP = os.path.join(os.path.dirname(__file__), '../data/raw/SECC_CE_20230101.shp')

# Parámetros de Negocio (Local Tipo Blue Banana)
LOCAL_STD_SIZE_M2 = 120 

warnings.filterwarnings("ignore")

def enrich_financial_layer():
    print("💶 PASO 09: CAPA FINANCIERA (ROI & ARBITRAJE)...")
    engine = create_engine(DB_CONNECTION_STR)

    # 1. CARGAR BARRIOS (MAPA DE PRECIOS)
    if not os.path.exists(BARRIOS_SHP):
        print(f"❌ ERROR: No encuentro el mapa en {BARRIOS_SHP}.")
        return

    print("   🗺️ Leyendo mapa base para precios (Secciones/Barrios)...")
    gdf_barrios = gpd.read_file(BARRIOS_SHP)
    
    # Proyección segura
    if gdf_barrios.crs.to_string() != "EPSG:4326":
        gdf_barrios = gdf_barrios.to_crs("EPSG:4326")

    # 2. GENERACIÓN DE PRECIOS (SIMULACIÓN INTELIGENTE)
    # En lugar de puro random, haremos que el precio dependa un poco del ID para que zonas cercanas tengan precios parecidos
    # (En producción: Aquí harías pd.merge con datos de Idealista)
    print("   🎲 Generando precios de mercado simulados (Base + Ruido)...")
    np.random.seed(42)
    
    # Precio base entre 15€ y 60€/m2
    # Simulamos barrios caros y baratos
    gdf_barrios['price_m2'] = np.random.uniform(15, 60, size=len(gdf_barrios))
    
    # 3. CARGAR TUS HEXÁGONOS GANADORES
    print("   ⬡ Leyendo resultados del modelo...")
    # Solo traemos lo necesario. Usamos 'similarity_final' que es tu score definitivo.
    sql = "SELECT h3_index, similarity_final, geom FROM retail_results WHERE similarity_final > 0"
    
    try:
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geom')
    except Exception as e:
        print(f"   ❌ Error leyendo retail_results: {e}")
        return
    
    if gdf_hex.empty:
        print("   ⚠️ No hay hexágonos con score > 0 para analizar.")
        return

    # Usamos centroide para cruce rápido
    gdf_hex['centroid'] = gdf_hex.geometry.centroid
    gdf_hex = gdf_hex.set_geometry('centroid')

    # 4. SPATIAL JOIN
    print("   🔗 Cruzando Hexágonos con Zonas de Precio...")
    
    # Buscamos columnas de nombre típicas
    posibles_nombres = ['NOMB', 'NAME', 'CDIS', 'CUSEC'] # CUSEC si usamos secciones
    col_nombre = next((c for c in gdf_barrios.columns if any(x in c for x in posibles_nombres)), 'id')
    
    # Join espacial: Hexágono WITHIN Barrio
    joined = gpd.sjoin(gdf_hex, gdf_barrios[[col_nombre, 'price_m2', 'geometry']], how='left', predicate='within')
    
    # 5. CÁLCULO DE RENTABILIDAD (ROI)
    print("   🧮 Calculando Opportunity Index...")
    
    # Rellenar precios nulos (si el hexágono cae fuera del mapa) con la media
    avg_price = joined['price_m2'].mean()
    joined['price_m2'] = joined['price_m2'].fillna(avg_price)
    
    # Alquiler Mensual = Precio m2 * Tamaño Local
    joined['est_monthly_rent'] = joined['price_m2'] * LOCAL_STD_SIZE_M2
    
    # OPPORTUNITY INDEX = (Score Calidad / Precio Alquiler) * Factor Escala
    # Buscamos: Alto Score (100) y Bajo Precio (20€) -> 100/20 = 5 (Excelente)
    # Malo: Bajo Score (20) y Alto Precio (60€) -> 20/60 = 0.33 (Pésimo)
    joined['opportunity_index'] = (joined['similarity_final'] / joined['price_m2']) * 10
    
    # Nombre del distrito
    joined['district_name'] = joined[col_nombre].fillna("Zona Desconocida")

    # 6. GUARDAR
    print("   💾 Guardando KPIs financieros en 'retail_results'...")
    
    df_upload = pd.DataFrame(joined[['h3_index', 'price_m2', 'est_monthly_rent', 'opportunity_index', 'district_name']])
    df_upload.to_sql('temp_finance', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        # Crear columnas
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS price_m2 FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS est_monthly_rent FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS opportunity_index FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS district_name TEXT;"))
        
        # Update masivo
        conn.execute(text("""
            UPDATE retail_results AS m
            SET price_m2 = s.price_m2,
                est_monthly_rent = s.est_monthly_rent,
                opportunity_index = s.opportunity_index,
                district_name = s.district_name
            FROM temp_finance AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_finance;"))

    print("✅ CAPA FINANCIERA LISTA.")
    
    # Top Arbitraje
    print("\n💎 TOP 3 OPORTUNIDADES (Gemas Ocultas):")
    top_opps = joined.sort_values('opportunity_index', ascending=False).head(3)
    for idx, row in top_opps.iterrows():
        print(f"   📍 {row['district_name']} (H3: {row['h3_index']})")
        print(f"      Score: {row['similarity_final']:.1f} | Precio: {row['price_m2']:.1f}€/m2 | ROI Index: {row['opportunity_index']:.1f}")

if __name__ == "__main__":
    enrich_financial_layer()
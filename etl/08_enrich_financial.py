import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import numpy as np
import os
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
# Ruta exacta donde tienes tu shapefile
BARRIOS_SHP = "data/raw/BARRIOS.shp" 

# Parámetros de Negocio (Local Tipo Blue Banana)
LOCAL_STD_SIZE_M2 = 120 

warnings.filterwarnings("ignore")

def enrich_financial_layer():
    print("💶 PASO 08: CAPA FINANCIERA (SPATIAL JOIN & ROI)...")
    engine = create_engine(DB_URL)

    # 1. CARGAR BARRIOS (TU CAPA NUEVA)
    if not os.path.exists(BARRIOS_SHP):
        print(f"❌ ERROR: No encuentro {BARRIOS_SHP}. Revisa la ruta.")
        return

    print("   🗺️  Leyendo mapa de Barrios...")
    gdf_barrios = gpd.read_file(BARRIOS_SHP)
    
    # Aseguramos proyección WGS84 para cruzar con H3
    if gdf_barrios.crs.to_string() != "EPSG:4326":
        print("      -> Reproyectando barrios a EPSG:4326...")
        gdf_barrios = gdf_barrios.to_crs("EPSG:4326")

    # 2. SIMULACIÓN DE PRECIOS (MOCKUP)
    # En producción, harías un merge con un CSV de Idealista aquí.
    # Ahora, generamos precios aleatorios pero "con sentido" para el MVP.
    print("   🎲 Generando precios de mercado simulados (15€ - 60€/m2)...")
    np.random.seed(42) # Semilla fija para que siempre salgan los mismos precios
    
    # Creamos columna de precio random
    gdf_barrios['price_m2'] = np.random.uniform(15, 60, size=len(gdf_barrios))
    
    # 3. CARGAR TUS HEXÁGONOS (RESULTADOS)
    print("   ⬡  Leyendo tus hexágonos ganadores...")
    # Leemos la tabla final 'retail_results'
    sql = "SELECT h3_index, similarity, geom FROM retail_results"
    gdf_hex = gpd.read_postgis(sql, engine, geom_col='geom')
    
    # Para el Spatial Join, usamos el centroide del hexágono (es más rápido y seguro)
    gdf_hex['centroid'] = gdf_hex.geometry.centroid
    gdf_hex = gdf_hex.set_geometry('centroid')

    # 4. SPATIAL JOIN (CRUCE MÁGICO)
    # "Asigna a cada hexágono los datos del barrio que lo contiene"
    print("   🔗 Cruzando: ¿En qué barrio cae cada hexágono?")
    
    # op='within' -> El centroide está DENTRO del barrio
    # Mantenemos columnas clave del barrio. Ajusta 'NOMBRE' al nombre real de la columna en tu SHP
    # (Suele ser 'CODBDT', 'NOMBRE', 'DESCRIPCIO', etc. Busca una de texto).
    
    # Intentamos adivinar la columna de nombre
    col_nombre = next((c for c in gdf_barrios.columns if 'NOM' in c or 'NAM' in c), 'id')
    print(f"      -> Usando columna de nombre de barrio: {col_nombre}")
    
    joined = gpd.sjoin(gdf_hex, gdf_barrios[[col_nombre, 'price_m2', 'geometry']], how='left', predicate='within')
    
    # 5. CÁLCULO DE RENTABILIDAD (ROI TEÓRICO)
    print("   🧮 Calculando KPIs Financieros...")
    
    # Alquiler Mensual Estimado
    joined['est_monthly_rent'] = joined['price_m2'] * LOCAL_STD_SIZE_M2
    
    # OPPORTUNITY SCORE (La métrica de arbitraje)
    # Fórmula: Calidad (Score) / Precio. 
    # Buscamos mucho Score por poco Precio.
    # Multiplicamos por 10 para que sea legible (ej: 8.5)
    joined['opportunity_index'] = (joined['similarity'] / joined['price_m2']) * 10
    
    # Limpieza de nulos (hexágonos que caen fuera de barrios oficiales)
    joined['est_monthly_rent'] = joined['est_monthly_rent'].fillna(0)
    joined['opportunity_index'] = joined['opportunity_index'].fillna(0)
    joined['district_name'] = joined[col_nombre].fillna("Desconocido")

    # 6. GUARDAR EN BBDD
    print("   💾 Actualizando 'retail_results' con datos financieros...")
    
    # Subir datos a tabla temporal
    df_upload = pd.DataFrame(joined[['h3_index', 'price_m2', 'est_monthly_rent', 'opportunity_index', 'district_name']])
    df_upload.to_sql('temp_finance', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Crear columnas
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS price_m2 FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS est_monthly_rent FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS opportunity_index FLOAT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS district_name TEXT;"))
        
        # Update
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
        conn.commit()

    print("✅ CAPA FINANCIERA LISTA. Datos de alquiler integrados.")
    
    # Mostrar el Top Oportunidad
    print("\n💎 TOP 3 GEMAS (Alta Calidad / Bajo Precio):")
    print(joined.sort_values('opportunity_index', ascending=False).head(3)[['h3_index', 'similarity', 'est_monthly_rent']])

if __name__ == "__main__":
    enrich_financial_layer()
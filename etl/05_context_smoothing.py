import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

def apply_smoothing():
    print("🔄 PASO 05: SUAVIZADO ESPACIAL (CONTEXTO K-RING)...")
    engine = create_engine(DB_URL)

    # 1. LEER DATOS COMPLETOS
    print("   Leyendo tabla enriquecida...")
    # Solo traemos lo que existe: Renta y Población Target
    # Coalesce(0) para evitar nulos que rompen las sumas
    sql = """
    SELECT 
        h3_index, 
        COALESCE(target_pop, 0) as target_pop, 
        COALESCE(avg_income, 0) as avg_income 
    FROM retail_hexagons_enriched
    """
    df = pd.read_sql(sql, engine)
    
    # Crear diccionarios para búsqueda ultrarrápida
    pop_dict = df.set_index('h3_index')['target_pop'].to_dict()
    inc_dict = df.set_index('h3_index')['avg_income'].to_dict()
    
    # 2. ALGORITMO DE SUAVIZADO (K-RING)
    print("   Calculando promedios vecinales...")
    
    results = []
    total = len(df)
    
    for idx, row in df.iterrows():
        h3_ix = row['h3_index']
        # Obtenemos vecinos inmediatos (k=1) -> El central + 6 vecinos
        neighbors = h3.k_ring(h3_ix, 1)
        
        # Variables acumuladores
        sum_pop = 0
        sum_inc = 0
        count_inc = 0
        
        for n in neighbors:
            if n in pop_dict: # Solo si el vecino existe en nuestros datos
                # Lógica de Negocio:
                # Población: SUMA (Catchment Area) -> Cuanta más gente alrededor, mejor.
                sum_pop += pop_dict[n]
                
                # Renta: PROMEDIO (Average) -> Solo sumamos si tiene dato (>0) para no diluir con parques
                val_inc = inc_dict[n]
                if val_inc > 0:
                    sum_inc += val_inc
                    count_inc += 1
        
        # Calculamos finales
        final_pop = sum_pop # Catchment acumulado
        final_inc = sum_inc / count_inc if count_inc > 0 else 0 # Renta media de la zona
        
        results.append({
            'h3_index': h3_ix,
            'target_pop_smooth': final_pop,
            'income_smooth': final_inc
        })

    df_smooth = pd.DataFrame(results)

    # 3. GUARDAR (UPDATE)
    print("💾 Guardando variables 'smooth' en BBDD...")
    
    df_smooth.to_sql('temp_smooth', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Crear columnas
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS target_pop_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS income_smooth FLOAT;"))
        
        # Update masivo
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET target_pop_smooth = s.target_pop_smooth,
                income_smooth = s.income_smooth
            FROM temp_smooth AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_smooth;"))
        conn.commit()

    print("✅ CONTEXTO AÑADIDO. Los datos ya no son islas.")
    print(df_smooth.head(3))

if __name__ == "__main__":
    apply_smoothing()
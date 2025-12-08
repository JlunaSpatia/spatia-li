import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings
import numpy as np

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# CONFIGURACIÓN DE PONDERACIÓN (K=2)
# Anillo 0 (Centro): 100%
# Anillo 1 (Vecinos): 60%
# Anillo 2 (Periferia): 30%
WEIGHTS = {0: 1.0, 1: 0.6, 2: 0.3}

def apply_smoothing_pro():
    print("🔄 PASO 05: SUAVIZADO ESPACIAL (CATCHMENT AREAS)...")
    engine = create_engine(DB_URL)

    # 1. LEER DATOS
    # NOTA: No usamos COALESCE en vacancy_rate. 
    # Queremos distinguir entre 0.0 (Calle muerta) y NULL (Sin datos/Otra ciudad).
    print("   Leyendo tabla enriquecida...")
    sql = """
    SELECT 
        h3_index, 
        COALESCE(target_pop, 0) as target_pop, 
        COALESCE(avg_income, 0) as avg_income,
        COALESCE(gravity_score, 0) as gravity_score,
        vacancy_rate 
    FROM retail_hexagons_enriched
    """
    df = pd.read_sql(sql, engine)
    
    # Diccionarios para búsqueda rápida (O(1))
    pop_dict = df.set_index('h3_index')['target_pop'].to_dict()
    inc_dict = df.set_index('h3_index')['avg_income'].to_dict()
    grav_dict = df.set_index('h3_index')['gravity_score'].to_dict()
    vac_dict = df.set_index('h3_index')['vacancy_rate'].to_dict() # Puede contener NaNs
    
    # 2. ALGORITMO DE SUAVIZADO
    print("   Calculando promedios ponderados y sumas vecinales...")
    
    results = []
    
    for idx, row in df.iterrows():
        h3_ix = row['h3_index']
        
        # A. ACUMULADORES DE VOLUMEN (SUMA)
        w_sum_pop = 0
        w_sum_grav = 0
        
        # B. ACUMULADORES DE CUALIDAD (PROMEDIO)
        w_sum_inc = 0
        total_weight_inc = 0 
        
        w_sum_vac = 0
        total_weight_vac = 0
        
        # Obtenemos anillos
        k_rings = h3.k_ring_distances(h3_ix, 2)
        
        for k, ring_set in enumerate(k_rings):
            current_weight = WEIGHTS.get(k, 0)
            
            for neighbor in ring_set:
                if neighbor in pop_dict:
                    # --- Lógica de Volumen (Suma) ---
                    w_sum_pop += pop_dict[neighbor] * current_weight
                    w_sum_grav += grav_dict[neighbor] * current_weight
                    
                    # --- Lógica de Cualidad (Renta) ---
                    val_inc = inc_dict[neighbor]
                    if val_inc > 0:
                        w_sum_inc += val_inc * current_weight
                        total_weight_inc += current_weight
                    
                    # --- Lógica de Cualidad (Vacancy - NUEVO) ---
                    # CRÍTICO: Solo procesamos si hay dato real (no es NaN)
                    val_vac = vac_dict.get(neighbor)
                    if pd.notna(val_vac): 
                        w_sum_vac += val_vac * current_weight
                        total_weight_vac += current_weight
        
        # CÁLCULOS FINALES
        
        # Renta: Promedio Ponderado
        final_inc = w_sum_inc / total_weight_inc if total_weight_inc > 0 else 0
        
        # Vacancy: Promedio Ponderado (Manejo de NULLs para ciudades sin datos)
        if total_weight_vac > 0:
            final_vac = w_sum_vac / total_weight_vac
        else:
            final_vac = None # Si estamos en Valencia, se queda como NULL

        results.append({
            'h3_index': h3_ix,
            'target_pop_smooth': w_sum_pop,
            'gravity_smooth': w_sum_grav,
            'income_smooth': final_inc,
            'vacancy_smooth': final_vac
        })

    df_smooth = pd.DataFrame(results)

    # 3. GUARDAR EN BBDD
    print("💾 Volcando resultados...")
    
    df_smooth.to_sql('temp_smooth', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Crear columnas si no existen
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS target_pop_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS gravity_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS income_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS vacancy_smooth FLOAT;"))
        
        print("   Ejecutando UPDATE masivo...")
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET target_pop_smooth = s.target_pop_smooth,
                gravity_smooth = s.gravity_smooth,
                income_smooth = s.income_smooth,
                vacancy_smooth = s.vacancy_smooth
            FROM temp_smooth AS s
            WHERE m.h3_index = s.h3_index;
        """))
        
        conn.execute(text("DROP TABLE temp_smooth;"))
        conn.commit()

    print("✅ SUAVIZADO COMPLETADO (Incluye Vacancy Rate donde existe).")

if __name__ == "__main__":
    apply_smoothing_pro()
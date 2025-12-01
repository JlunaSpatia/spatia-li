import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# CONFIGURACIÓN DE PONDERACIÓN (K=2)
# Anillo 0 (Centro): 100% de valor
# Anillo 1 (Vecinos inmediatos): 60% de valor
# Anillo 2 (Vecinos lejanos): 30% de valor
WEIGHTS = {0: 1.0, 1: 0.6, 2: 0.3}

def apply_smoothing_pro():
    print("🔄 PASO 06: SUAVIZADO ESPACIAL PRO (K=2 PONDERADO)...")
    engine = create_engine(DB_URL)

    # 1. LEER DATOS
    print("   Leyendo tabla enriquecida...")
    sql = """
    SELECT 
        h3_index, 
        COALESCE(target_pop, 0) as target_pop, 
        COALESCE(avg_income, 0) as avg_income,
        COALESCE(gravity_score, 0) as gravity_score
    FROM retail_hexagons_enriched
    """
    df = pd.read_sql(sql, engine)
    
    # Diccionarios para velocidad
    pop_dict = df.set_index('h3_index')['target_pop'].to_dict()
    inc_dict = df.set_index('h3_index')['avg_income'].to_dict()
    grav_dict = df.set_index('h3_index')['gravity_score'].to_dict()
    
    # 2. ALGORITMO DE SUAVIZADO PONDERADO
    print("   Calculando catchment areas (Radio ~12 min)...")
    
    results = []
    
    for idx, row in df.iterrows():
        h3_ix = row['h3_index']
        
        # ACUMULADORES PONDERADOS
        w_sum_pop = 0
        w_sum_grav = 0
        w_sum_inc = 0
        total_weight_inc = 0 # Para normalizar el promedio de renta
        
        # Obtenemos anillos por distancia (k_ring_distances devuelve el anillo y la distancia k)
        # hex_ring es un diccionario: {h3_index: k_distance}
        k_rings = h3.k_ring_distances(h3_ix, 2)
        
        # k_rings devuelve una lista de sets: [set_k0, set_k1, set_k2]
        for k, ring_set in enumerate(k_rings):
            current_weight = WEIGHTS.get(k, 0)
            
            for neighbor in ring_set:
                if neighbor in pop_dict:
                    # VARIABLES DE VOLUMEN (SUMA PONDERADA)
                    # "La gente lejos cuenta menos"
                    w_sum_pop += pop_dict[neighbor] * current_weight
                    w_sum_grav += grav_dict[neighbor] * current_weight
                    
                    # VARIABLE DE CUALIDAD (PROMEDIO PONDERADO)
                    # "La renta se promedia, pero pesa más la cercana"
                    val_inc = inc_dict[neighbor]
                    if val_inc > 0:
                        w_sum_inc += val_inc * current_weight
                        total_weight_inc += current_weight
        
        # Calcular Renta Final (Promedio Ponderado)
        final_inc = w_sum_inc / total_weight_inc if total_weight_inc > 0 else 0
        
        results.append({
            'h3_index': h3_ix,
            'target_pop_smooth': w_sum_pop,
            'gravity_smooth': w_sum_grav,
            'income_smooth': final_inc
        })

    df_smooth = pd.DataFrame(results)

    # 3. GUARDAR (UPDATE)
    print("💾 Guardando en BBDD...")
    df_smooth.to_sql('temp_smooth', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS target_pop_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS gravity_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS income_smooth FLOAT;"))
        
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET target_pop_smooth = s.target_pop_smooth,
                gravity_smooth = s.gravity_smooth,
                income_smooth = s.income_smooth
            FROM temp_smooth AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_smooth;"))
        conn.commit()

    print("✅ CATCHMENT AREA (K=2) APLICADO.")

if __name__ == "__main__":
    apply_smoothing_pro()
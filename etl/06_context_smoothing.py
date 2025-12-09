import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings
import os
import sys

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

warnings.filterwarnings("ignore")

# CONFIGURACIÓN DE PONDERACIÓN (K=2)
# Anillo 0 (Centro): 100% | Anillo 1: 60% | Anillo 2: 30%
WEIGHTS = {0: 1.0, 1: 0.6, 2: 0.3}

def apply_smoothing_pro():
    print("🔄 PASO 06: SUAVIZADO ESPACIAL (CONTEXT SMOOTHING)...")
    engine = create_engine(DB_CONNECTION_STR)

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
    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        print(f"   ❌ Error leyendo base de datos: {e}")
        return
    
    if df.empty:
        print("   ⚠️ No hay datos para suavizar.")
        return

    # Diccionarios para búsqueda rápida (O(1))
    pop_dict = df.set_index('h3_index')['target_pop'].to_dict()
    inc_dict = df.set_index('h3_index')['avg_income'].to_dict()
    grav_dict = df.set_index('h3_index')['gravity_score'].to_dict()
    
    # 2. ALGORITMO DE SUAVIZADO
    print(f"   Calculando contextos para {len(df)} hexágonos...")
    
    results = []
    total_rows = len(df)
    
    for idx, row in df.iterrows():
        if idx % 1000 == 0: print(f"      Procesando {idx}/{total_rows}...", end="\r")

        h3_ix = row['h3_index']
        
        # A. ACUMULADORES DE VOLUMEN (SUMA) -> "Cuanto más alrededor, mejor"
        w_sum_pop = 0
        w_sum_grav = 0
        
        # B. ACUMULADORES DE CUALIDAD (PROMEDIO) -> "El nivel medio de la zona"
        w_sum_inc = 0
        total_weight_inc = 0 
        
        try:
            # Obtenemos anillos (Centro + Vecinos + Periferia)
            k_rings = h3.k_ring_distances(h3_ix, 2)
        except:
             continue

        for k, ring_set in enumerate(k_rings):
            current_weight = WEIGHTS.get(k, 0)
            if current_weight == 0: continue
            
            for neighbor in ring_set:
                if neighbor in pop_dict: # Solo si el vecino existe en nuestros datos
                    # --- Lógica de Volumen (Suma) ---
                    w_sum_pop += pop_dict[neighbor] * current_weight
                    w_sum_grav += grav_dict[neighbor] * current_weight
                    
                    # --- Lógica de Cualidad (Promedio) ---
                    val_inc = inc_dict[neighbor]
                    if val_inc > 0:
                        w_sum_inc += val_inc * current_weight
                        total_weight_inc += current_weight
        
        # CÁLCULOS FINALES
        # Renta: Si estoy rodeado de ricos, mi 'income_smooth' sube aunque yo sea un parque
        final_inc = w_sum_inc / total_weight_inc if total_weight_inc > 0 else 0
        
        results.append({
            'h3_index': h3_ix,
            'target_pop_smooth': w_sum_pop,
            'gravity_smooth': w_sum_grav,
            'income_smooth': final_inc
        })

    df_smooth = pd.DataFrame(results)

    # 3. GUARDAR EN BBDD
    print("\n💾 Volcando resultados...")
    
    df_smooth.to_sql('temp_smooth', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn: 
        # Crear columnas si no existen
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS target_pop_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS gravity_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS income_smooth FLOAT;"))
        
        print("   Ejecutando UPDATE masivo...")
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET target_pop_smooth = s.target_pop_smooth,
                gravity_smooth = s.gravity_smooth,
                income_smooth = s.income_smooth
            FROM temp_smooth AS s
            WHERE m.h3_index = s.h3_index;
        """))
        
        conn.execute(text("DROP TABLE temp_smooth;"))

    print("✅ SUAVIZADO COMPLETADO (Pop, Income, Gravity).")

if __name__ == "__main__":
    apply_smoothing_pro()
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
    # Traemos las 3 variables clave que hemos generado
    sql = "SELECT h3_index, pop_2025, avg_income, gravity_score FROM retail_hexagons_enriched"
    df = pd.read_sql(sql, engine)
    
    # Rellenar nulos con 0 para poder sumar
    df = df.fillna(0)
    
    # Crear diccionarios para búsqueda ultrarrápida
    pop_dict = df.set_index('h3_index')['pop_2025'].to_dict()
    inc_dict = df.set_index('h3_index')['avg_income'].to_dict()
    grav_dict = df.set_index('h3_index')['gravity_score'].to_dict()
    
    # 2. ALGORITMO DE SUAVIZADO (K-RING)
    print("   Calculando promedios vecinales (esto tarda un pelín)...")
    
    results = []
    total = len(df)
    
    for idx, row in df.iterrows():
        h3_ix = row['h3_index']
        # Obtenemos vecinos inmediatos (k=1) -> El central + 6 vecinos
        neighbors = h3.k_ring(h3_ix, 1)
        
        # Variables acumuladores
        sum_pop = 0
        sum_inc = 0
        sum_grav = 0
        count = 0
        
        for n in neighbors:
            if n in pop_dict: # Solo si el vecino existe en nuestros datos
                sum_pop += pop_dict[n]
                sum_inc += inc_dict[n]
                sum_grav += grav_dict[n]
                count += 1
        
        # Lógica de Negocio:
        # Población y Gravedad: SUMA (Catchment Area) -> Cuanta más gente alrededor, mejor.
        # Renta: PROMEDIO (Average) -> Que vivas al lado de ricos te hace zona rica.
        
        results.append({
            'h3_index': h3_ix,
            'pop_smooth': sum_pop,           # Suma del vecindario
            'gravity_smooth': sum_grav,      # Suma del vecindario
            'income_smooth': sum_inc / count if count > 0 else 0 # Promedio del vecindario
        })

    df_smooth = pd.DataFrame(results)

    # 3. GUARDAR (UPDATE)
    print("💾 Guardando variables 'smooth' en BBDD...")
    
    df_smooth.to_sql('temp_smooth', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Crear columnas
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS pop_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS gravity_smooth FLOAT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS income_smooth FLOAT;"))
        
        # Update
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET pop_smooth = s.pop_smooth,
                gravity_smooth = s.gravity_smooth,
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
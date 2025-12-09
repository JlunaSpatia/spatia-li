import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
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

# PARÁMETROS DE NEGOCIO
# Esta distancia asegura diversidad geográfica.
MIN_DISTANCE_METERS = 1000  # Distancia mínima entre candidatos
TOP_N_PER_CITY = 10         # Top 10 por ciudad

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula distancia en metros para vectores numpy"""
    R = 6371000 
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def tag_top_locations_per_city():
    print(f"💎 SELECCIONANDO TOP {TOP_N_PER_CITY} ZONAS INDEPENDIENTES POR CIUDAD...")
    engine = create_engine(DB_CONNECTION_STR)
    
    # 1. LEER RESULTADOS DEL MODELO (Desde retail_results que ya tiene el score final)
    print("   Leyendo resultados del modelo...")
    try:
        query = """
        SELECT h3_index, city, lat, lon, similarity_final 
        FROM retail_results 
        WHERE similarity_final > 0 
        ORDER BY city, similarity_final DESC
        """
        df_all = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Error leyendo retail_results: {e}")
        return
    
    if df_all.empty:
        print("❌ Error: No hay resultados con Score > 0.")
        return

    global_winners = []

    # 2. PROCESO ITERATIVO POR CIUDAD (Algoritmo Greedy NMS)
    cities = df_all['city'].unique()
    print(f"   Ciudades encontradas: {list(cities)}")

    for city in cities:
        print(f"   📍 Procesando {city}...")
        
        # Filtramos ciudad
        df_city = df_all[df_all['city'] == city].copy()
        
        city_winners = []
        rank_counter = 1
        
        # Bucle: Mientras queramos más ganadores y queden candidatos
        while len(city_winners) < TOP_N_PER_CITY and not df_city.empty:
            
            # A. El mejor candidato actual (siempre es la fila 0 porque está ordenado)
            best_candidate = df_city.iloc[0]
            
            # Guardamos
            city_winners.append({
                'h3_index': best_candidate['h3_index'],
                'selection_rank': rank_counter,
                'selection_label': f"Top {rank_counter} {city.split(',')[0]}", # Limpiar nombre "Madrid, Spain" -> "Madrid"
                'final_score': best_candidate['similarity_final']
            })
            
            # B. Calculamos distancias contra el resto de candidatos
            dists = haversine_distance(
                df_city['lat'].values, df_city['lon'].values,
                best_candidate['lat'], best_candidate['lon']
            )
            
            # C. SUPRESIÓN DE VECINOS
            # Filtramos el DF quedándonos solo con lo que está LEJOS (> 1km)
            df_city = df_city[dists > MIN_DISTANCE_METERS]
            
            rank_counter += 1
            
        global_winners.extend(city_winners)
        print(f"      -> Seleccionados {len(city_winners)} finalistas.")

    # 3. ACTUALIZAR BBDD
    if not global_winners:
        print("⚠️ No se seleccionó ningún candidato.")
        return

    df_winners = pd.DataFrame(global_winners)
    
    print("💾 Actualizando tabla maestra 'retail_results'...")
    
    # Subimos ganadores a temp
    df_winners.to_sql('temp_top_winners', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        # A. Crear columnas en la tabla de RESULTADOS (retail_results)
        # Es mejor tenerlo aquí para el Dashboard
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS selection_rank INT;"))
        conn.execute(text("ALTER TABLE retail_results ADD COLUMN IF NOT EXISTS selection_label TEXT;"))
        
        # B. Resetear ranks anteriores
        conn.execute(text("UPDATE retail_results SET selection_rank = NULL, selection_label = NULL;"))
        
        # C. Update Final
        print("      -> Marcando ganadores...")
        sql_update = """
        UPDATE retail_results AS m
        SET 
            selection_rank = t.selection_rank,
            selection_label = t.selection_label
        FROM temp_top_winners AS t
        WHERE m.h3_index = t.h3_index;
        """
        conn.execute(text(sql_update))
        
        conn.execute(text("DROP TABLE temp_top_winners;"))

    print("✅ PROCESO COMPLETADO.")
    print("   Ahora tu Dashboard mostrará solo las chinchetas ganadoras.")
    
    # Preview
    print("\n🏆 PODIO FINAL:")
    print(df_winners[['selection_label', 'final_score', 'h3_index']].head(5))

if __name__ == "__main__":
    tag_top_locations_per_city()
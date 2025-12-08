import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

# PARÁMETROS DE NEGOCIO
MIN_DISTANCE_METERS = 1000  # Distancia mínima entre candidatos (para no repetir zonas)
TOP_N_PER_CITY = 10         # Queremos el Top 10 por ciudad

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
    engine = create_engine(DB_URL)
    
    # 1. LEER RESULTADOS DEL MODELO
    # Leemos de 'retail_results' que tiene el score final calculado
    print("   Leyendo resultados del modelo...")
    query = """
    SELECT h3_index, city, lat, lon, similarity_final 
    FROM retail_results 
    WHERE similarity_final > 0 
    ORDER BY city, similarity_final DESC
    """
    df_all = pd.read_sql(query, engine)
    
    if df_all.empty:
        print("❌ Error: No hay resultados para procesar.")
        return

    # Lista para guardar los ganadores de todas las ciudades
    global_winners = []

    # 2. PROCESO ITERATIVO POR CIUDAD
    cities = df_all['city'].unique()
    print(f"   Ciudades encontradas: {list(cities)}")

    for city in cities:
        print(f"   📍 Procesando {city}...")
        
        # Filtramos solo la ciudad actual
        df_city = df_all[df_all['city'] == city].copy()
        
        city_winners = []
        rank_counter = 1
        
        # ALGORITMO GREEDY (Non-Maximum Suppression)
        while len(city_winners) < TOP_N_PER_CITY and not df_city.empty:
            
            # A. El mejor candidato actual (Fila 0 tras ordenar)
            best_candidate = df_city.iloc[0]
            
            # Guardamos su ID y su Ranking
            city_winners.append({
                'h3_index': best_candidate['h3_index'],
                'selection_rank': rank_counter,
                'selection_label': f"Top {rank_counter} {city}"
            })
            
            # B. Calculamos distancias contra el resto
            dists = haversine_distance(
                df_city['lat'].values, df_city['lon'].values,
                best_candidate['lat'], best_candidate['lon']
            )
            
            # C. ELIMINAMOS VECINOS CERCANOS (< 1km)
            # Esto "limpia" la zona para buscar el siguiente líder real
            df_city = df_city[dists > MIN_DISTANCE_METERS]
            
            rank_counter += 1
            
        # Añadimos los ganadores de esta ciudad a la lista global
        global_winners.extend(city_winners)
        print(f"      -> Encontrados {len(city_winners)} candidatos independientes.")

    # 3. ACTUALIZAR LA BASE DE DATOS (TAGGING)
    if not global_winners:
        print("⚠️ No se seleccionó ningún candidato.")
        return

    df_winners = pd.DataFrame(global_winners)
    
    print("💾 Actualizando tabla maestra 'retail_hexagons_enriched'...")
    
    # Escribimos los ganadores en una tabla temporal
    df_winners.to_sql('temp_top_winners', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # A. Crear columnas de Tagging si no existen
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS selection_rank INT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS selection_label TEXT;"))
        
        # B. LIMPIEZA PREVIA: Borrar tags antiguos para que el Top sea fresco
        # (Ponemos todo a NULL antes de marcar los nuevos)
        conn.execute(text("UPDATE retail_hexagons_enriched SET selection_rank = NULL, selection_label = NULL;"))
        
        # C. UPDATE FINAL: Cruzar tabla temporal con la maestra
        sql_update = """
        UPDATE retail_hexagons_enriched AS m
        SET 
            selection_rank = t.selection_rank,
            selection_label = t.selection_label
        FROM temp_top_winners AS t
        WHERE m.h3_index = t.h3_index;
        """
        conn.execute(text(sql_update))
        
        # Limpiar
        conn.execute(text("DROP TABLE temp_top_winners;"))
        conn.commit()

    print("✅ PROCESO COMPLETADO.")
    print("   Ahora puedes filtrar en tu mapa: WHERE selection_rank IS NOT NULL")
    
    # Preview de control
    print("\n🏆 EJEMPLO DE GANADORES:")
    print(df_winners.head())

if __name__ == "__main__":
    tag_top_locations_per_city()
import sys
import os
from sqlalchemy import create_engine, text

# Aseguramos que Python encuentre el motor en la carpeta services
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.isochrone_service import IsochroneService
from conf import DB_URL

def ejecutar_estudio_consultoria(nombre_estudio, lista_locales, minutos=15):
    engine = create_engine(DB_URL)
    service = IsochroneService(DB_URL)
    
    print(f"🛠️  Iniciando estudio: {nombre_estudio}")

    # 1. Calculamos las isócronas (se guardan temporalmente en catchment_areas)
    puntos_formateados = [{'id': l['name'], 'lat': l['lat'], 'lon': l['lon']} for l in lista_locales]
    num_procesados = service.calculate_and_save(puntos_formateados, minutes=minutos)
    
    if num_procesados > 0:
        # 2. Movemos los datos a tu tabla de estudios organizada
        ids_estudio = ", ".join([f"'{l['name']}'" for l in lista_locales])
        
        with engine.connect() as conn:
            # Insertamos en la tabla final con el nombre del estudio
            conn.execute(text(f"""
                INSERT INTO analytics.study_catchments (study_name, location_name, minutes, geometry)
                SELECT :study_name, origin_id, minutes, geometry 
                FROM analytics.catchment_areas 
                WHERE origin_id IN ({ids_estudio});
            """), {"study_name": nombre_estudio})
            
            # Limpiamos la tabla temporal para que no se ensucie
            conn.execute(text(f"DELETE FROM analytics.catchment_areas WHERE origin_id IN ({ids_estudio});"))
            conn.commit()
        
        print(f"✅ ¡Éxito! {num_procesados} locales guardados en 'analytics.study_catchments'.")

if __name__ == "__main__":
    # --- CONFIGURA AQUÍ TU ANÁLISIS ---
    TITULO = "EXPANSION_MADRID_TEST"
    MINS = 10 # Tiempo caminando
    
    COORDENADAS_CLIENTE = [
        {'name': 'Moncloa', 'lat': 40.43464674138137, 'lon': -3.7155552246801644},
        {'name': 'Cuzco', 'lat': 40.45874540593397, 'lon': -3.6918884176774815},
        {'name': 'Salamanca', 'lat': 40.43254363165603, 'lon': -3.6795784120936146}
    ]
    # ----------------------------------

    ejecutar_estudio_consultoria(TITULO, COORDENADAS_CLIENTE, MINS)
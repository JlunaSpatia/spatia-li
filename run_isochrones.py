from sqlalchemy import create_engine, text
from services.isochrone_service import IsochroneService
from conf import DB_URL

def correr_estudio_cliente(nombre_estudio, locales, mins):
    engine = create_engine(DB_URL)
    service = IsochroneService(DB_URL)
    
    print(f"🚀 Iniciando Local Pulse: {nombre_estudio}")
    
    # El servicio guarda en analytics.catchment_areas temporalmente
    service.calculate_and_save(
        locales, 
        minutes=mins, 
        table_name="catchment_areas", 
        schema="analytics", 
        id_column="location_id"
    )
    
    with engine.begin() as conn:
        # Movemos al histórico de estudios
        conn.execute(text("""
            INSERT INTO analytics.study_catchments (study_name, location_name, minutes, geometry)
            SELECT :study, location_id, minutes, geometry FROM analytics.catchment_areas;
        """), {"study": nombre_estudio})
        
        # Limpiamos temporal
        conn.execute(text("DELETE FROM analytics.catchment_areas;"))
        
    print(f"✅ Estudio '{nombre_estudio}' completado al 100%.")

if __name__ == "__main__":
    MIS_LOCALES = [
        {
            'id': 'Justicia_AugustoFigueroa_Hipster', 
            'lat': 40.42265211333806, 
            'lon': -3.699011996918335
        },
        {
            'id': 'Salamanca_JorgeJuan_Premium', 
            'lat': 40.42345766471462, 
            'lon': -3.675626995378436
        },
        {
            'id': 'Chamberi_Ponzano_Trendy', 
            'lat': 40.44184053268655, 
            'lon': -3.6990991182542063
        },
        {
            'id': 'Arganzuela_Delicias_Gentrificacion', 
            'lat': 40.39931274691982, 
            'lon': -3.694027256882531
        }
    ]

    # Aquí llamarías a tu función de estudio:
    # ejecutar_estudio_consultoria("GYM_BOUTIQUE_HYPE_MADRID", MIS_LOCALES, minutos=10)

    correr_estudio_cliente("GYM_BOUTIQUE_MADRID_001", MIS_LOCALES, mins=10)
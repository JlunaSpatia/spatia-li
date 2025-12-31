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
        {'id': 'Las Tablas Zona VPL', 'lat': 40.50101968445583, 'lon': -3.6756266837367515},
        {'id': 'Las Tablas Zona telecinco', 'lat': 40.51096818743371, 'lon':  -3.676583140892681}
    ]


    correr_estudio_cliente("VALIDACION_MADRID_RETAIL_2", MIS_LOCALES, mins=10)
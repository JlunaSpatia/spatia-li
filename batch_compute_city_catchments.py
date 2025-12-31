import pandas as pd
from sqlalchemy import create_engine
from services.isochrone_service import IsochroneService
from conf import DB_URL

def mapear_ciudad_completa(ciudad, mins):
    engine = create_engine(DB_URL)
    service = IsochroneService(DB_URL)
    
    # Buscamos hexágonos sin procesar en core
    query = f"""
        SELECT h.h3_id, ST_Y(ST_Centroid(h.geometry)) as lat, ST_X(ST_Centroid(h.geometry)) as lon 
        FROM core.hexagons h
        LEFT JOIN core.catchment_{mins}m c ON h.h3_id = c.h3_id
        WHERE h.city = '{ciudad}' AND c.h3_id IS NULL
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print(f"✅ {ciudad} ya está procesada para {mins} min.")
        return

    # Procesamos por bloques para no saturar OSRM
    batch_size = 500
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        puntos = [{'id': r['h3_id'], 'lat': r['lat'], 'lon': r['lon']} for _, r in batch.iterrows()]
        
        service.calculate_and_save(
            puntos, 
            minutes=mins, 
            table_name=f"catchment_{mins}m", 
            schema="core", 
            id_column="h3_id"
        )
        print(f"📦 Progreso {ciudad}: {i+len(batch)}/{len(df)}")

if __name__ == "__main__":
    mapear_ciudad_completa("MADRID", mins=15)
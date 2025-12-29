import sys
import pandas as pd
from sqlalchemy import create_engine
from services.isochrone_service import IsochroneService
from conf import DB_URL

def mapear_geometrias_ciudad(nombre_ciudad, minutos):
    engine = create_engine(DB_URL)
    service = IsochroneService(DB_URL)
    
    # 1. Definir la tabla de destino dinámicamente según los minutos
    # Esto te permite tener tablas diferentes: catchment_15m, catchment_10m...
    table_name = f"catchment_{minutos}m"
    
    # 2. Buscar hexágonos pendientes
    query = f"""
        SELECT h.h3_id, ST_Y(ST_Centroid(h.geometry)) as lat, ST_X(ST_Centroid(h.geometry)) as lon 
        FROM core.hexagons h
        LEFT JOIN core.{table_name} c ON h.h3_id = c.h3_id
        WHERE h.city = '{nombre_ciudad}' AND c.h3_id IS NULL
    """
    pendientes = pd.read_sql(query, engine)
    
    if pendientes.empty:
        print(f"✅ {nombre_ciudad} ya está mapeada al completo para {minutos} min.")
        return

    print(f"🗺️  Iniciando proceso: {nombre_ciudad} | Tiempo: {minutos} min | Pendientes: {len(pendientes)}")

    batch_size = 500
    for i in range(0, len(pendientes), batch_size):
        batch = pendientes.iloc[i : i + batch_size]
        puntos = [{'id': row['h3_id'], 'lat': row['lat'], 'lon': row['lon']} for _, row in batch.iterrows()]
        
        try:
            # El servicio guarda directamente en la tabla configurada
            service.calculate_and_save(puntos, minutes=minutos, table_name=table_name)
            print(f"📦 Batch {i//batch_size + 1}/{len(pendientes)//batch_size + 1} completado.")
        except Exception as e:
            print(f"❌ Error en batch {i//batch_size + 1}: {e}")

if __name__ == "__main__":
    # ==========================================
    # ### CONFIGURACIÓN MANUAL ###
    # ==========================================
    CIUDAD_POR_DEFECTO = "MADRID"
    MINUTOS_POR_DEFECTO = 15
    # ==========================================

    # Leer argumentos de terminal si existen: python script.py BARCELONA 10
    ciudad = sys.argv[1] if len(sys.argv) > 1 else CIUDAD_POR_DEFECTO
    minutos = int(sys.argv[2]) if len(sys.argv) > 2 else MINUTOS_POR_DEFECTO

    mapear_geometrias_ciudad(ciudad, minutos)
import pandas as pd
import requests
from sqlalchemy import create_engine
import sys

# -----------------------------
# CONFIGURACIÓN
# -----------------------------
# Conexión a la BBDD (igual que en tu otro script)
DB_CONN = "postgresql://postgres:postgres@localhost:5432/spatia"

# URL de OSRM (Backend de Coche)
OSRM_URL = "http://localhost:5000/table/v1/driving/"

# PARÁMETROS DEL ANÁLISIS
# Ejemplo: Puerta del Sol (-3.7038, 40.4168)
ORIGEN_LAT = 40.4168
ORIGEN_LON = -3.7038
TIEMPO_MAX_MINUTOS = 5

# Nombre de la tabla donde se guardará el resultado
TABLA_DESTINO = "analisis_isocrona_temp"

def main():
    print(f"--- INICIANDO CÁLCULO DE ISOCRONA ({TIEMPO_MAX_MINUTOS} MIN) ---")
    
    # 1. CONEXIÓN A BBDD
    try:
        engine = create_engine(DB_CONN)
    except Exception as e:
        print(f"Error conectando a PostGIS: {e}")
        sys.exit(1)

    # 2. FILTRO PREVIO (BUFFER GEOGRÁFICO)
    # Calculamos un radio "seguro" para no saturar OSRM.
    # 15 min a 100km/h son 25km. Usamos 20km de radio para filtrar candidatos.
    radio_metros = 20000 
    
    print("1. Buscando hexágonos candidatos en PostGIS (Radio aéreo)...")
    
    sql_candidatos = f"""
    SELECT 
        h3index, 
        ST_X(centroid) as lon, 
        ST_Y(centroid) as lat 
    FROM dim_h3
    WHERE ST_DWithin(
        centroid::geography, 
        ST_MakePoint({ORIGEN_LON}, {ORIGEN_LAT})::geography, 
        {radio_metros}
    )
    AND res = 8; -- Asegúrate de tener hexágonos de esta resolución
    """
    
    df_candidatos = pd.read_sql(sql_candidatos, engine)
    
    if df_candidatos.empty:
        print("❌ No se encontraron hexágonos cerca. Revisa que 'dim_h3' tenga datos y las coordenadas sean correctas.")
        return

    print(f"   -> Encontrados {len(df_candidatos)} hexágonos candidatos.")

    # 3. CONSULTA A OSRM (POR LOTES / CHUNKS)
    # Las URLs tienen límite de longitud, así que procesamos de 100 en 100.
    CHUNK_SIZE = 100
    resultados = []
    
    # El origen siempre es el primer punto
    coord_origen = f"{ORIGEN_LON},{ORIGEN_LAT}"
    
    print("2. Consultando tiempos reales a OSRM...")
    
    # Iteramos sobre el dataframe en trozos
    for i in range(0, len(df_candidatos), CHUNK_SIZE):
        chunk = df_candidatos.iloc[i : i + CHUNK_SIZE]
        
        # Construimos la lista de coordenadas: Origen + Destinos del chunk
        coords_list = [coord_origen] 
        ids_chunk = []
        
        for _, row in chunk.iterrows():
            coords_list.append(f"{row['lon']},{row['lat']}")
            ids_chunk.append(row['h3index'])
            
        coords_string = ";".join(coords_list)
        
        # sources=0 -> El primero es origen, el resto destinos
        url = f"{OSRM_URL}{coords_string}?sources=0&annotations=duration"
        
        try:
            r = requests.get(url)
            if r.status_code != 200:
                print(f"   ⚠️ Error en lote {i}: Status {r.status_code}")
                continue
                
            data = r.json()
            if data['code'] != 'Ok':
                print(f"   ⚠️ Error OSRM: {data.get('message')}")
                continue
                
            # Procesar tiempos (ignoramos el primero que es 0)
            tiempos = data['durations'][0][1:]
            
            for idx_hex, segundos in enumerate(tiempos):
                if segundos is not None:
                    minutos = segundos / 60
                    if minutos <= TIEMPO_MAX_MINUTOS:
                        resultados.append({
                            "h3index": ids_chunk[idx_hex],
                            "minutos_viaje": round(minutos, 2)
                        })
                        
        except Exception as e:
            print(f"   ⚠️ Error de conexión: {e}")

    # 4. GUARDAR RESULTADOS
    print(f"3. Procesamiento finalizado. Hexágonos válidos: {len(resultados)}")
    
    if resultados:
        df_final = pd.DataFrame(resultados)
        
        # Guardamos en PostGIS
        df_final.to_sql(TABLA_DESTINO, engine, if_exists='replace', index=False)
        print(f"✅ ÉXITO: Tabla '{TABLA_DESTINO}' creada/actualizada.")
        print("   Ahora puedes visualizarla en QGIS uniendo con 'dim_h3'.")
    else:
        print("⚠️ No se encontraron hexágonos dentro del tiempo límite.")

if __name__ == "__main__":
    main()
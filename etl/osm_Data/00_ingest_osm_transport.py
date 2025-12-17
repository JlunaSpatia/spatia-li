import osmium
import sys
import os
from sqlalchemy import create_engine, text
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

# ================= SETUP DE RUTAS =================
current_dir = os.path.dirname(os.path.abspath(__file__))
# Ajustar para llegar a la raíz del proyecto
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

# Ruta al PBF
PBF_PATH = os.path.join(project_root, "data", "raw", "spain-251213.osm.pbf")

try:
    from config import DB_CONNECTION_STR
except ImportError:
    sys.exit("❌ Error: No encuentro config.py")

# ================= EL MANEJADOR (STREAMING) =================
class TransportHandler(osmium.SimpleHandler):
    def __init__(self):
        super(TransportHandler, self).__init__()
        self.buffer = []
        self.total_nodes = 0
        self.extracted_count = 0
        
    def node(self, n):
        self.total_nodes += 1
        # Filtro de etiquetas (Tags) - Lo mismo que hacíamos antes pero en stream
        is_transport = False
        
        # Comprobaciones rápidas (Highway, Public Transport, Railway)
        if 'highway' in n.tags and n.tags['highway'] == 'bus_stop':
            is_transport = True
        elif 'public_transport' in n.tags and n.tags['public_transport'] in ['platform', 'station', 'stop_position']:
            is_transport = True
        elif 'railway' in n.tags and n.tags['railway'] in ['station', 'subway_entrance', 'tram_stop']:
            is_transport = True
            
        if is_transport:
            # Extraemos los datos básicos
            # OJO: n.location.lon/lat deben leerse aquí, el objeto 'n' se destruye después
            try:
                geom = Point(n.location.lon, n.location.lat)
                name = n.tags.get('name', 'Unknown')
                
                self.buffer.append({
                    'id': n.id,
                    'highway': n.tags.get('highway'),
                    'public_transport': n.tags.get('public_transport'),
                    'railway': n.tags.get('railway'),
                    'name': name,
                    'geometry': geom  # Objeto Shapely
                })
                self.extracted_count += 1
            except:
                pass # Ignorar nodos con geometría corrupta

# ================= FUNCIÓN PRINCIPAL =================
def ingest_data():
    if not os.path.exists(PBF_PATH):
        sys.exit(f"❌ No encuentro el archivo: {PBF_PATH}")

    print(f"🚀 MODO STREAMING: Procesando {PBF_PATH}")
    print("⏳ Esto tardará un poco, pero NO ocupará RAM y NO se colgará.")
    
    # 1. Preparar Base de Datos
    engine = create_engine(DB_CONNECTION_STR)
    table_name = "osm_transport_points"
    
    # Borrar tabla vieja si existe para empezar limpio
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
        conn.commit()
    
    # 2. Inicializar el Handler
    h = TransportHandler()
    
    # 3. Procesar el archivo (Aquí ocurre la magia del streaming)
    # apply_file recorre el archivo de principio a fin
    print("🌊 Leyendo archivo (puedes ver Netflix, esto no explotará)...")
    try:
        h.apply_file(PBF_PATH, locations=False) # locations=False para nodos es más rápido
    except Exception as e:
        sys.exit(f"❌ Error leyendo PBF: {e}")

    print(f"✅ Lectura terminada. Escaneados: {h.total_nodes} nodos.")
    print(f"📦 Encontrados: {len(h.buffer)} paradas de transporte.")

    # 4. Guardar en DB (En lotes para no saturar)
    if h.buffer:
        print("🛢️ Guardando en PostGIS...")
        # Creamos DataFrame
        df = pd.DataFrame(h.buffer)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        
        # Subir a PostGIS
        # chunksize ayuda a subirlo poco a poco
        gdf.to_postgis(table_name, engine, if_exists='replace', index=False, chunksize=5000)
        
        # 5. Crear Índice Espacial
        print("⚡ Creando índice espacial...")
        with engine.connect() as conn:
            conn.execute(text(f"CREATE INDEX idx_{table_name}_geom ON {table_name} USING GIST (geometry);"))
            conn.commit()
            
        print("🎉 ¡PROCESO COMPLETADO CON ÉXITO!")
    else:
        print("⚠️ No se encontraron datos de transporte. Revisa los filtros.")

if __name__ == "__main__":
    ingest_data()
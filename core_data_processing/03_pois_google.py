import sys
import os
import ijson  # <--- NECESARIO: pip install ijson
import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# IMPORTAMOS EL NUEVO NOMBRE DEL ARCHIVO
try:
    from conf import DB_URL # <--- CAMBIADO DE config A conf
except ImportError:
    print("⚠️ No pude importar conf.py. Asegúrate de haber renombrado config.py a conf.py")
    DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

TARGET_FOLDER = "2025_Q4"
FILE_SUFFIX = "_FULL_RAW.json"
RAW_POIS_DIR = os.path.join(BASE_DIR, "data", "raw", TARGET_FOLDER)

SCHEMA = "core"
TABLE_FINAL = "pois"
TABLE_STAGING = "pois_staging"
BATCH_SIZE = 2000 # Procesamos de 2000 en 2000 para no gastar RAM

def setup_database(engine):
    """Prepara tablas."""
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_FINAL} (
                place_id TEXT PRIMARY KEY,
                name TEXT, category TEXT, rating FLOAT, reviews FLOAT, 
                price TEXT, address TEXT, lat FLOAT, lng FLOAT,
                city_source TEXT, source_file TEXT, release_quarter TEXT,
                h3_id TEXT, geometry geometry(Point, 4326)
            );
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_STAGING};"))
        conn.execute(text(f"CREATE TABLE {SCHEMA}.{TABLE_STAGING} (LIKE {SCHEMA}.{TABLE_FINAL} INCLUDING ALL);"))
        conn.commit()

def process_batch(engine, batch_rows):
    """Sube un lote pequeño a Staging y lo mueve a Final."""
    if not batch_rows: return
    
    df = pd.DataFrame(batch_rows)
    
    # 1. Subir a Staging
    df.to_sql(TABLE_STAGING, engine, schema=SCHEMA, if_exists='append', index=False)
    
    # 2. Mover a Final (Ignorando duplicados)
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE_FINAL} 
            SELECT * FROM {SCHEMA}.{TABLE_STAGING}
            ON CONFLICT (place_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                reviews = EXCLUDED.reviews,
                release_quarter = EXCLUDED.release_quarter;
        """))
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE_STAGING};"))
        conn.commit()

def update_geometry_and_h3(engine):
    """Cálculos finales en SQL."""
    print("🌍 Calculando geometría y H3...")
    with engine.connect() as conn:
        conn.execute(text(f"""
            UPDATE {SCHEMA}.{TABLE_FINAL} SET geometry = ST_SetSRID(ST_MakePoint(lng, lat), 4326)
            WHERE geometry IS NULL AND lat IS NOT NULL;
        """))
        conn.execute(text(f"""
            UPDATE {SCHEMA}.{TABLE_FINAL} p SET h3_id = h.h3_id
            FROM {SCHEMA}.hexagons h
            WHERE p.h3_id IS NULL AND p.geometry IS NOT NULL AND ST_Within(p.geometry, h.geometry);
        """))
        conn.commit()

def extract_item_data(item, city_source, filename):
    """Limpia un solo item."""
    # Extracción segura de ID
    pid = str(item.get('placeId') or item.get('id') or item.get('place_id') or '').strip()
    if not pid: return None

    # Extracción segura de Coordenadas
    lat, lng = None, None
    if 'lat' in item: lat = item['lat']
    elif 'latitude' in item: lat = item['latitude']
    
    if 'lng' in item: lng = item['lng']
    elif 'lon' in item: lng = item['lon']
    elif 'longitude' in item: lng = item['longitude']

    if lat is None and 'location' in item and isinstance(item.get('location'), dict):
        lat = item['location'].get('lat')
        lng = item['location'].get('lng')

    try:
        lat = float(lat)
        lng = float(lng)
    except:
        return None

    return {
        'place_id': pid,
        'name': str(item.get('title') or item.get('name') or '').strip(),
        'category': str(item.get('categoryName') or item.get('category') or '').strip(),
        'rating': float(item.get('totalScore') or item.get('rating') or 0),
        'reviews': float(item.get('reviewsCount') or item.get('reviews') or 0),
        'price': str(item.get('price') or '').strip(),
        'address': str(item.get('address') or '').strip(),
        'lat': lat,
        'lng': lng,
        'city_source': city_source,
        'source_file': filename,
        'release_quarter': TARGET_FOLDER,
        'h3_id': None
    }

def load_pois_layer():
    engine = create_engine(DB_URL)
    setup_database(engine)
    
    import glob
    search_pattern = os.path.join(RAW_POIS_DIR, f"*{FILE_SUFFIX}")
    files = glob.glob(search_pattern)

    print(f"🚀 INICIANDO STREAMING (Bajo consumo RAM)... Archivos: {len(files)}")

    for f in files:
        filename = os.path.basename(f)
        city_source = filename.replace(FILE_SUFFIX, "").replace("_", " ").title()
        print(f"\n   📖 Leyendo: {city_source} (Modo Stream)...")

        batch = []
        count = 0
        
        try:
            with open(f, 'rb') as input_file: # Modo binario para ijson
                # ijson lee uno a uno sin cargar todo el archivo
                # Intentamos leer items directamente
                parser = ijson.items(input_file, 'item') 
                
                # Fallback por si la estructura es distinta
                try:
                    first = next(parser)
                    from itertools import chain
                    iterator = chain([first], parser)
                except:
                    input_file.seek(0)
                    iterator = ijson.items(input_file, 'results.item')

                for item in iterator:
                    clean_row = extract_item_data(item, city_source, filename)
                    if clean_row:
                        batch.append(clean_row)
                        count += 1
                    
                    # Si llenamos el batch, guardamos y vaciamos memoria
                    if len(batch) >= BATCH_SIZE:
                        process_batch(engine, batch)
                        print(f"      ↳ Guardados {count} POIs...", end='\r')
                        batch = [] # Liberar RAM

                # Procesar el resto final
                if batch:
                    process_batch(engine, batch)
                    
            print(f"      ✅ Finalizado: {count} POIs totales.")

        except Exception as e:
            print(f"      ❌ Error leyendo archivo: {e}")

    update_geometry_and_h3(engine)
    print("\n✅ PROCESO COMPLETADO EXITOSAMENTE.")

if __name__ == "__main__":
    load_pois_layer()
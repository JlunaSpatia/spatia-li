import sys
import os
import ijson
import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL
except ImportError:
    print("⚠️ Usando DB default local.")
    DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

TARGET_FOLDER = "2025_Q4"
FILE_SUFFIX = "_FULL_RAW.json"
RAW_POIS_DIR = os.path.join(BASE_DIR, "data", "raw", TARGET_FOLDER)

SCHEMA = "core"
TABLE_FINAL = "pois"
TABLE_STAGING = "pois_staging"
BATCH_SIZE = 2000 

def setup_database(engine):
    """Reinicia la tabla staging y asegura la final."""
    with engine.connect() as conn:
        # Tabla Final (Estricta)
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_FINAL} (
                place_id TEXT PRIMARY KEY,
                name TEXT, category TEXT, rating FLOAT, reviews FLOAT, 
                price TEXT, address TEXT, lat FLOAT, lng FLOAT,
                city_source TEXT, source_file TEXT, release_quarter TEXT,
                h3_id TEXT, geometry geometry(Point, 4326)
            );
        """))
        
        # Tabla Staging (Flexible: Sin Primary Key estricta para facilitar la carga inicial)
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_STAGING};"))
        conn.execute(text(f"""
            CREATE TABLE {SCHEMA}.{TABLE_STAGING} (
                place_id TEXT, -- NO ES PK AQUÍ PARA EVITAR ERRORES DE INSERT
                name TEXT, category TEXT, rating FLOAT, reviews FLOAT, 
                price TEXT, address TEXT, lat FLOAT, lng FLOAT,
                city_source TEXT, source_file TEXT, release_quarter TEXT,
                h3_id TEXT, geometry geometry(Point, 4326)
            );
        """))
        conn.commit()

def process_batch(engine, batch_rows):
    """Sube datos a BBDD."""
    if not batch_rows: return
    
    # 1. Deduplicación en Memoria (Python)
    # Si en el mismo batch viene el mismo ID dos veces, nos quedamos con el último
    df = pd.DataFrame(batch_rows).drop_duplicates(subset='place_id', keep='last')
    
    if df.empty: return

    # 2. Subir a Staging (Ahora seguro sin duplicados internos)
    df.to_sql(TABLE_STAGING, engine, schema=SCHEMA, if_exists='append', index=False)
    
    # 3. Merge a Final (Deduplicación contra la BBDD)
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE_FINAL} 
            SELECT DISTINCT ON (place_id) * FROM {SCHEMA}.{TABLE_STAGING}
            ON CONFLICT (place_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                reviews = EXCLUDED.reviews,
                release_quarter = EXCLUDED.release_quarter,
                source_file = EXCLUDED.source_file;
        """))
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE_STAGING};"))
        conn.commit()

def update_geometry_and_h3(engine):
    print("\n🌍 Calculando geometría y H3...")
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
    if not isinstance(item, dict): return None

    # ID
    pid = str(item.get('place_id') or item.get('placeId') or '').strip()
    if not pid: return None

    # Coordenadas
    lat, lng = None, None
    if 'gps_coordinates' in item and isinstance(item['gps_coordinates'], dict):
        lat = item['gps_coordinates'].get('latitude')
        lng = item['gps_coordinates'].get('longitude')
    
    if lat is None:
        lat = item.get('latitude')
        lng = item.get('longitude')

    try:
        lat = float(lat)
        lng = float(lng)
    except:
        return None 

    return {
        'place_id': pid,
        'name': str(item.get('title') or item.get('name') or '').strip(),
        'category': str(item.get('type') or item.get('category') or '').strip(),
        'rating': float(item.get('rating') or 0),
        'reviews': float(item.get('reviews') or 0),
        'price': str(item.get('price') or '').strip(),
        'address': str(item.get('address') or '').strip(),
        'lat': lat,
        'lng': lng,
        'city_source': city_source,
        'source_file': filename,
        'release_quarter': TARGET_FOLDER,
        'h3_id': None
    }

def get_json_path(filepath):
    """Detecta estructura JSON."""
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(1024)
                if not chunk: break
                s = chunk.decode('utf-8', errors='ignore').strip()
                if not s: continue
                if s.startswith('['): return 'item'
                if s.startswith('{'): return 'results.item' 
                break
    except: pass
    return 'item'

def load_pois_layer():
    engine = create_engine(DB_URL)
    setup_database(engine)
    
    import glob
    search_pattern = os.path.join(RAW_POIS_DIR, f"*{FILE_SUFFIX}")
    files = glob.glob(search_pattern)

    print(f"🚀 INICIANDO CARGA SEGURA - Archivos: {len(files)}")

    for f in files:
        filename = os.path.basename(f)
        city_source = filename.replace(FILE_SUFFIX, "").replace("_", " ").title()
        json_path = get_json_path(f)
        
        print(f"\n📖 Leyendo: {city_source} (Ruta: {json_path})...")

        batch = []
        count = 0
        
        try:
            with open(f, 'rb') as input_file:
                # Intentamos lectura robusta
                try:
                    parser = ijson.items(input_file, json_path)
                    # Forzamos lectura del primero para ver si falla
                    first = next(parser)
                    from itertools import chain
                    iterator = chain([first], parser)
                except StopIteration:
                    # Estructura vacía o diferente
                    input_file.seek(0)
                    try:
                        iterator = ijson.items(input_file, 'item')
                        first = next(iterator)
                        iterator = chain([first], iterator)
                    except:
                        iterator = []

                for item in iterator:
                    clean_row = extract_item_data(item, city_source, filename)
                    
                    if clean_row:
                        batch.append(clean_row)
                        count += 1
                        
                        if len(batch) >= BATCH_SIZE:
                            process_batch(engine, batch)
                            print(f"      ↳ Procesados: {count}...", end='\r')
                            batch = []

                if batch: process_batch(engine, batch)
            
            print(f"   ✅ Finalizado: {count} POIs guardados.")

        except Exception as e:
            print(f"   ❌ Error archivo: {e}")

    update_geometry_and_h3(engine)
    print("\n✅ PROCESO COMPLETADO.")

if __name__ == "__main__":
    load_pois_layer()
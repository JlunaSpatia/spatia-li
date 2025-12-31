import sys
import os
import ijson
import pandas as pd
from sqlalchemy import create_engine, text
import glob

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL, ACTIVE_CITIES
except ImportError:
    print("⚠️ No se pudo cargar conf.py. Usando valores default.")
    DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
    ACTIVE_CITIES = []

TARGET_FOLDER = "2025_Q4"
FILE_SUFFIX = "_FULL_RAW.json"
RAW_POIS_DIR = os.path.join(BASE_DIR, "data", "raw", TARGET_FOLDER)

SCHEMA = "core"
TABLE_FINAL = "pois"
TABLE_STAGING = "pois_staging"
BATCH_SIZE = 2000 

# ==========================================
# 2. FUNCIONES BASE DE DATOS
# ==========================================
def setup_database(engine):
    """Prepara tablas y asegura que exista la columna reviews_link."""
    with engine.connect() as conn:
        # 1. Tabla Final (Si no existe)
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_FINAL} (
                place_id TEXT PRIMARY KEY,
                name TEXT, category TEXT, rating FLOAT, reviews FLOAT, 
                price TEXT, address TEXT, lat FLOAT, lng FLOAT,
                reviews_link TEXT,
                city_source TEXT, source_file TEXT, release_quarter TEXT,
                h3_id TEXT, geometry geometry(Point, 4326)
            );
        """))
        
        # 2. Auto-migración (Añadir columna si falta)
        conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE_FINAL} ADD COLUMN IF NOT EXISTS reviews_link TEXT;"))
        
        # 3. Tabla Staging (Siempre limpia al inicio)
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_STAGING};"))
        conn.execute(text(f"""
            CREATE TABLE {SCHEMA}.{TABLE_STAGING} (
                place_id TEXT, name TEXT, category TEXT, rating FLOAT, reviews FLOAT, 
                price TEXT, address TEXT, lat FLOAT, lng FLOAT, reviews_link TEXT,
                city_source TEXT, source_file TEXT, release_quarter TEXT,
                h3_id TEXT, geometry geometry(Point, 4326)
            );
        """))
        conn.commit()

def process_batch(engine, batch_rows):
    """
    Sube datos asegurando el mapeo exacto de columnas para evitar desorden.
    """
    if not batch_rows: return
    
    # 1. Deduplicar en Python
    df = pd.DataFrame(batch_rows).drop_duplicates(subset='place_id', keep='last')
    
    # 2. Subir a Staging
    df.to_sql(TABLE_STAGING, engine, schema=SCHEMA, if_exists='append', index=False)
    
    # 3. Definir columnas explícitamente (SOLUCIÓN AL ERROR DE DESPLAZAMIENTO)
    # Deben coincidir con las claves del diccionario en extract_item_data
    cols_list = [
        "place_id", "name", "category", "rating", "reviews", 
        "price", "address", "lat", "lng", "reviews_link",
        "city_source", "source_file", "release_quarter", 
        "h3_id"
        # geometry no se incluye aquí porque se calcula en el paso final,
        # y en staging entra como NULL o lat/lng
    ]
    cols_sql = ", ".join(cols_list)

    with engine.connect() as conn:
        # INSERT explícito mapeando columna a columna
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE_FINAL} ({cols_sql})
            SELECT DISTINCT ON (place_id) {cols_sql} 
            FROM {SCHEMA}.{TABLE_STAGING}
            ON CONFLICT (place_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                reviews = EXCLUDED.reviews,
                reviews_link = EXCLUDED.reviews_link,
                release_quarter = EXCLUDED.release_quarter,
                source_file = EXCLUDED.source_file,
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                address = EXCLUDED.address
            WHERE {SCHEMA}.{TABLE_FINAL}.release_quarter IS DISTINCT FROM EXCLUDED.release_quarter;
        """))
        # Limpiar staging
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE_STAGING};"))
        conn.commit()

def update_geometry_and_h3(engine):
    print("\n🌍 Actualizando geometría y H3 (solo registros nuevos)...")
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

# ==========================================
# 3. LECTURA Y PARSEO
# ==========================================
def extract_item_data(item, city_source, filename):
    if not isinstance(item, dict): return None
    
    # Obtener ID
    pid = str(item.get('place_id') or item.get('placeId') or '').strip()
    if not pid: return None

    # Obtener Coordenadas
    lat = item.get('gps_coordinates', {}).get('latitude')
    lng = item.get('gps_coordinates', {}).get('longitude')
    
    if lat is None:
        lat = item.get('latitude')
        lng = item.get('longitude')

    try:
        lat, lng = float(lat), float(lng)
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
        'lat': lat, 'lng': lng,
        'reviews_link': str(item.get('reviews_link') or '').strip(),
        'city_source': city_source,
        'source_file': filename,
        'release_quarter': TARGET_FOLDER,
        'h3_id': None
        # geometry no se pasa aquí, se calcula con update_geometry_and_h3
    }

def get_json_path(filepath):
    """Detecta estructura JSON (Lista [] o Objeto {results:...})."""
    try:
        with open(filepath, 'rb') as f:
            chunk = f.read(1024).decode('utf-8', errors='ignore').strip()
            if chunk.startswith('['): return 'item'
            if chunk.startswith('{'): return 'results.item'
    except: pass
    return 'item'

# ==========================================
# 4. PROCESO PRINCIPAL
# ==========================================
def load_pois_layer():
    engine = create_engine(DB_URL)
    setup_database(engine)
    
    # 1. Buscar archivos
    search_pattern = os.path.join(RAW_POIS_DIR, f"*{FILE_SUFFIX}")
    all_files = glob.glob(search_pattern)
    
    # 2. Filtrar por ACTIVE_CITIES
    files_to_process = []
    for f in all_files:
        clean_name = os.path.basename(f).replace(FILE_SUFFIX, "")
        if clean_name in ACTIVE_CITIES:
            files_to_process.append(f)

    if not files_to_process:
        print(f"❌ ERROR: No se encontraron archivos para ACTIVE_CITIES: {ACTIVE_CITIES}")
        return

    print(f"🚀 INICIANDO CARGA - Archivos encontrados: {len(files_to_process)}")

    for f in files_to_process:
        filename = os.path.basename(f)
        city_source = filename.replace(FILE_SUFFIX, "").replace("_", " ").title()
        
        # --- CHECK DE SEGURIDAD POR CIUDAD ---
        with engine.connect() as conn:
            res = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SCHEMA}.{TABLE_FINAL} 
                WHERE city_source = :city AND release_quarter = :q
            """), {"city": city_source, "q": TARGET_FOLDER})
            existing_count = res.scalar()

            if existing_count > 0:
                print(f"\n⚠️  {city_source}: Ya existen {existing_count} registros de la release {TARGET_FOLDER}.")
                opcion = input("   ¿Procesar para buscar locales nuevos? (Los existentes NO se modificarán) [s/n]: ")
                if opcion.lower() != 's':
                    print(f"   ⏭️  Saltando {city_source}...")
                    continue
        # -------------------------------------

        json_path = get_json_path(f)
        print(f"\n📖 Procesando: {city_source} (Archivo: {filename})...")

        batch = []
        count_inserted = 0
        
        try:
            with open(f, 'rb') as input_file:
                items = ijson.items(input_file, json_path)
                for item in items:
                    clean_row = extract_item_data(item, city_source, filename)
                    if clean_row:
                        batch.append(clean_row)
                        count_inserted += 1
                        
                        if len(batch) >= BATCH_SIZE:
                            process_batch(engine, batch)
                            print(f"     ↳ Insertados: {count_inserted}...", end='\r')
                            batch = []
                
                # Procesar el último lote
                if batch: process_batch(engine, batch)
            
            print(f"   ✅ Finalizado: {count_inserted} POIs insertados.")

        except Exception as e:
            print(f"   ❌ Error en {filename}: {e}")

    update_geometry_and_h3(engine)
    print("\n✅ PROCESO COMPLETADO.")

if __name__ == "__main__":
    load_pois_layer()
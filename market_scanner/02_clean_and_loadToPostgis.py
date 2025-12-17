import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURACIÓN ---
# Asegúrate de que DB_URL sea correcta en tu config.py o aquí
try:
    from config import DB_CONNECTION_STR
    DB_URL = DB_CONNECTION_STR
except ImportError:
    DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

CITY_NAME = "BARCELONA"
QUARTER = "2025_Q4"
TASK_ID_TO_LOG = 30

TABLES = {
    'master': 'retail_poi_master',
    'categories': 'poi_categories',
    'metadata': 'poi_metadata'
}

def register_etl_history(engine, task_id, run_date, status, log_output):
    """Registra auditoría."""
    query = text("""
        INSERT INTO etl_history (task_id, run_date, status, log_output)
        VALUES (:task_id, :run_date, :status, :log_output);
    """)
    try:
        with engine.connect() as conn:
            conn.execute(query, {
                'task_id': task_id, 'run_date': run_date, 'status': status, 'log_output': log_output
            })
            conn.commit()
        print(f"📝 Auditoría guardada: Status {status}")
    except Exception as e:
        print(f"❌ Error guardando auditoría (puede que la tabla no exista): {e}")

def clean_text_field(value):
    """Filtro de seguridad: Fuerza texto simple, evita listas y nulos."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return str(value[0]) if len(value) > 0 else None
    return str(value)

def safe_float(value):
    """Convierte con seguridad a float o devuelve None (NULL). Evita el error de ''."""
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (ValueError, TypeError):
        return None

def process_and_load():
    run_datetime = datetime.now()
    snapshot_date_iso = run_datetime.strftime("%Y-%m-%d") 
    
    print(f"🚀 INICIANDO ETL para {CITY_NAME} - Snapshot: {snapshot_date_iso}")

    # 1. VERIFICACIÓN DE DUPLICADOS (Carga Incremental)
    engine = create_engine(DB_URL)
    
    # Intentamos ver si ya cargamos hoy
    try:
        check_query = text(f"SELECT COUNT(*) FROM {TABLES['master']} WHERE city = :city AND snapshot_date = :date")
        with engine.connect() as conn:
            exists = conn.execute(check_query, {'city': CITY_NAME, 'date': snapshot_date_iso}).scalar()
        
        if exists and exists > 0:
            print(f"⚠️  AVISO: Ya existen {exists} registros para hoy. Se añadirán los nuevos (si los hay).")
            # No hacemos return para permitir reintentos o cargas parciales, 
            # pero en producción podrías querer parar aquí.
    except Exception as e:
        print(f"ℹ️ Primera carga o error verificando: {e}")

    # 2. CARGA Y LIMPIEZA EN MEMORIA
    # Ajusta la ruta si es necesario según tu estructura
    # raw_file = os.path.join("data", "raw", QUARTER, f"{CITY_NAME}_FULL_RAW.json")
    # Usamos ruta absoluta basada en donde suele estar el script para evitar fallos
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Sube a spatia-li
    raw_file = os.path.join(base_dir, "data", "raw", QUARTER, f"{CITY_NAME}_FULL_RAW.json")

    if not os.path.exists(raw_file):
        print(f"❌ Archivo RAW no encontrado: {raw_file}")
        return

    print("🧹 Procesando JSON Raw en memoria...")
    with open(raw_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    unique_pois = {}
    skipped_geo = 0
    
    # --- FASE 1: DEDUPLICACIÓN Y FILTRO GEOGRÁFICO ---
    for r in raw_data:
        if isinstance(r, dict) and 'place_id' in r:
            
            # Recuperar lat/lon con seguridad
            # A veces viene en 'gps_coordinates', a veces suelto en 'latitude'
            lat = None
            lon = None
            
            if 'gps_coordinates' in r and isinstance(r['gps_coordinates'], dict):
                lat = safe_float(r['gps_coordinates'].get('latitude'))
                lon = safe_float(r['gps_coordinates'].get('longitude'))
            elif 'latitude' in r:
                lat = safe_float(r.get('latitude'))
                lon = safe_float(r.get('longitude'))
            
            # FILTRO ANTI-MEXICO / NY
            # Barcelona está entre Lat 41.0 y 42.0 aprox.
            if lat is None or lon is None:
                continue # Sin coordenadas no sirve
            
            if not (41.0 < lat < 42.0) or not (1.0 < lon < 3.0):
                skipped_geo += 1
                continue # Fuera de Barcelona

            # DEDUPLICACIÓN POR ID
            pid = r['place_id']
            if pid not in unique_pois or len(r) > len(unique_pois[pid]):
                # Inyectamos las coordenadas limpias para usarlas luego
                r['_clean_lat'] = lat
                r['_clean_lon'] = lon
                unique_pois[pid] = r
    
    print(f"   📉 Reducción: {len(raw_data)} raw -> {len(unique_pois)} únicos.")
    print(f"   🌍 Descartados por estar fuera de BCN (NY/México): {skipped_geo}")

    master_rows = []
    cat_rows = []
    meta_rows = []

    # --- FASE 2: CONSTRUCCIÓN DE TABLAS ---
    for pid, r in unique_pois.items():
        
        # TABLA MASTER
        master_rows.append({
            'place_id': pid,
            'city': CITY_NAME,
            'snapshot_date': snapshot_date_iso,
            'title': clean_text_field(r.get('title')), 
            'rating': safe_float(r.get('rating')),           # <--- PROTEGIDO
            'reviews_count': safe_float(r.get('reviews')),   # <--- PROTEGIDO
            'price_level': clean_text_field(r.get('price')),
            'main_type': clean_text_field(r.get('type')),    # A veces es 'category' o 'main_type'
            'address': clean_text_field(r.get('address')),
            'latitude': r['_clean_lat'],
            'longitude': r['_clean_lon'],
            'website': clean_text_field(r.get('website')),
            'google_maps_url': clean_text_field(r.get('google_maps_url')),
            'search_category': clean_text_field(r.get('_category'))
        })
        
        # TABLA CATEGORIES
        # A veces viene como 'types' (lista) o 'subtype'
        tags = r.get('types', [])
        if not tags and 'subtypes' in r: tags = r['subtypes']
        
        if isinstance(tags, list):
            for tag in tags:
                cat_rows.append({
                    'place_id': pid,
                    'snapshot_date': snapshot_date_iso,
                    'category_tag': str(tag)
                })
            
        # TABLA METADATA
        meta_rows.append({
            'place_id': pid,
            'snapshot_date': snapshot_date_iso,
            'phone_number': clean_text_field(r.get('phone')),
            'operating_hours_json': json.dumps(r.get('operating_hours', {})),
            'image_url': clean_text_field(r.get('image')),
            'reviews_link': clean_text_field(r.get('reviews_link')),
            'photos_link': clean_text_field(r.get('photos_link')),
            'posts_link': clean_text_field(r.get('posts_link'))
        })

    # DataFrames
    df_master = pd.DataFrame(master_rows)
    df_cat = pd.DataFrame(cat_rows)
    df_meta = pd.DataFrame(meta_rows)

    # 3. CARGA A POSTGIS
    if df_master.empty:
        print("⚠️ No hay datos válidos para cargar.")
        return

    print("💾 Cargando a Base de Datos...")
    try:
        # Usamos chunksize para evitar timeouts con muchos datos
        df_master.to_sql(TABLES['master'], engine, if_exists='append', index=False, method='multi', chunksize=500)
        print(f"   ✅ Master: {len(df_master)} filas.")
        
        df_cat.to_sql(TABLES['categories'], engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"   ✅ Categories: {len(df_cat)} filas.")
        
        df_meta.to_sql(TABLES['metadata'], engine, if_exists='append', index=False, method='multi', chunksize=500)
        print(f"   ✅ Metadata: {len(df_meta)} filas.")

        # 4. GEOMETRÍA (Post-procesado)
        print("🌍 Generando geometrías PostGIS...")
        with engine.connect() as conn:
            # Asegurar que la columna existe
            conn.execute(text(f"ALTER TABLE {TABLES['master']} ADD COLUMN IF NOT EXISTS geometry geometry(Point, 4326);"))
            
            # Actualizar geometría solo para los nuevos registros de hoy
            sql_geo = text(f"""
                UPDATE {TABLES['master']}
                SET geometry = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                WHERE geometry IS NULL 
                  AND snapshot_date = :date 
                  AND city = :city
                  AND longitude IS NOT NULL;
            """)
            conn.execute(sql_geo, {'date': snapshot_date_iso, 'city': CITY_NAME})
            conn.commit()
        
        print("✅ Geometría actualizada.")
        
        msg = f"Carga Exitosa. Registros: {len(df_master)}"
        register_etl_history(engine, TASK_ID_TO_LOG, run_datetime, 'SUCCESS', msg)

    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN CARGA: {e}")
        register_etl_history(engine, TASK_ID_TO_LOG, run_datetime, 'FAILED', str(e)[0:255])

if __name__ == "__main__":
    process_and_load()
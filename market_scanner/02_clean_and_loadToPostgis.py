import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
CITY_NAME = "MADRID"
QUARTER = "2025_Q4"
TASK_ID_TO_LOG = 4

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
        print(f"❌ Error guardando auditoría: {e}")

def clean_text_field(value):
    """Filtro de seguridad: Fuerza texto simple, evita listas."""
    if value is None:
        return None
    if isinstance(value, list):
        return str(value[0]) if len(value) > 0 else None
    return str(value)

def process_and_load():
    run_datetime = datetime.now()
    snapshot_date_iso = run_datetime.strftime("%Y-%m-%d") 
    
    print(f"🚀 INICIANDO ETL para {CITY_NAME} - Snapshot: {snapshot_date_iso}")

    # 1. VERIFICACIÓN DE DUPLICADOS
    engine = create_engine(DB_URL)
    try:
        check_query = text(f"SELECT COUNT(*) FROM {TABLES['master']} WHERE city = :city AND snapshot_date = :date")
        with engine.connect() as conn:
            exists = conn.execute(check_query, {'city': CITY_NAME, 'date': snapshot_date_iso}).scalar()
        
        if exists and exists > 0:
            msg = f"ABORTADO: Ya existen {exists} registros para {CITY_NAME} con fecha {snapshot_date_iso}."
            print(f"🛑 {msg}")
            register_etl_history(engine, TASK_ID_TO_LOG, run_datetime, 'ABORTED', msg)
            return
    except Exception as e:
        print(f"ℹ️ Primera carga detectada: {e}")

    # 2. LIMPIEZA EN MEMORIA
    raw_file = os.path.join("data", "raw", QUARTER, f"{CITY_NAME}_FULL_RAW.json")
    if not os.path.exists(raw_file):
        print(f"❌ Archivo RAW no encontrado: {raw_file}")
        return

    print("🧹 Procesando JSON Raw en memoria...")
    with open(raw_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    unique_pois = {}
    for r in raw_data:
        if isinstance(r, dict) and 'place_id' in r:
            pid = r['place_id']
            if pid not in unique_pois or len(r) > len(unique_pois[pid]):
                unique_pois[pid] = r
    
    print(f"   📉 Reducción: {len(raw_data)} raw -> {len(unique_pois)} únicos.")

    master_rows = []
    cat_rows = []
    meta_rows = []

    for pid, r in unique_pois.items():
        # --- TABLA MASTER ---
        master_rows.append({
            'place_id': pid,
            'city': CITY_NAME,
            'snapshot_date': snapshot_date_iso,
            'title': clean_text_field(r.get('title')), 
            'rating': r.get('rating'),
            'reviews_count': r.get('reviews'),
            'price_level': clean_text_field(r.get('price')),
            'main_type': clean_text_field(r.get('type')),
            'address': clean_text_field(r.get('address')),
            'latitude': r.get('gps_coordinates', {}).get('latitude'),
            'longitude': r.get('gps_coordinates', {}).get('longitude'),
            'website': clean_text_field(r.get('website')),
            'google_maps_url': clean_text_field(r.get('google_maps_url')),
            'search_category': clean_text_field(r.get('_category'))
        })
        
        # --- TABLA CATEGORIES ---
        for tag in r.get('types', []):
            cat_rows.append({
                'place_id': pid,
                'snapshot_date': snapshot_date_iso,
                'category_tag': str(tag)
            })
            
        # --- TABLA METADATA (AQUÍ AÑADIMOS LOS LINKS) ---
        meta_rows.append({
            'place_id': pid,
            'snapshot_date': snapshot_date_iso,
            'phone_number': clean_text_field(r.get('phone')),
            'operating_hours_json': json.dumps(r.get('operating_hours', {})),
            'image_url': clean_text_field(r.get('image')),
            # NUEVOS CAMPOS RECUPERADOS:
            'reviews_link': clean_text_field(r.get('reviews_link')),
            'photos_link': clean_text_field(r.get('photos_link')),
            'posts_link': clean_text_field(r.get('posts_link'))
        })

    # DataFrames
    df_master = pd.DataFrame(master_rows)
    df_cat = pd.DataFrame(cat_rows)
    df_meta = pd.DataFrame(meta_rows)

    # 3. CARGA A POSTGIS
    print("💾 Cargando a Base de Datos...")
    try:
        df_master.to_sql(TABLES['master'], engine, if_exists='append', index=False)
        print(f"   ✅ Master: {len(df_master)} filas.")
        
        df_cat.to_sql(TABLES['categories'], engine, if_exists='append', index=False)
        print(f"   ✅ Categories: {len(df_cat)} filas.")
        
        df_meta.to_sql(TABLES['metadata'], engine, if_exists='append', index=False)
        print(f"   ✅ Metadata: {len(df_meta)} filas (con Links API).")

        # 4. GEOMETRÍA
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {TABLES['master']} ADD COLUMN IF NOT EXISTS geometry geometry(Point, 4326);"))
            conn.execute(text(f"""
                UPDATE {TABLES['master']}
                SET geometry = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                WHERE geometry IS NULL AND snapshot_date = '{snapshot_date_iso}' AND city = '{CITY_NAME}';
            """))
            conn.commit()
        
        print("🌍 Geometría generada exitosamente.")
        
        msg = f"Carga Exitosa. Master: {len(df_master)}. Snapshot: {snapshot_date_iso}"
        register_etl_history(engine, TASK_ID_TO_LOG, run_datetime, 'SUCCESS', msg)

    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN CARGA: {e}")
        register_etl_history(engine, TASK_ID_TO_LOG, run_datetime, 'FAILED', str(e))

if __name__ == "__main__":
    process_and_load()
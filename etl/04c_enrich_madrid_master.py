import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import glob
import os
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
H3_RES = 9
RAW_DIR = "data/raw"
PATTERN = "MADRID_MASTER_CENSUS_*.csv" # Busca archivos con fecha

warnings.filterwarnings("ignore")

def get_latest_file():
    """Busca el archivo Master más reciente en la carpeta"""
    files = glob.glob(os.path.join(RAW_DIR, PATTERN))
    if not files: return None
    return sorted(files)[-1]

def categorize_activity(row):
    """Clasifica la actividad en grupos estratégicos"""
    texto = (str(row.get('desc_seccion', '')) + " " + 
             str(row.get('desc_division', '')) + " " + 
             str(row.get('rotulo', ''))).upper()
    
    if 'NAN' in texto: texto = ""

    if any(x in texto for x in ['PRENDA', 'VESTIR', 'CALZADO', 'CUERO', 'TEXTIL', 'MODA', 'JOYERIA', 'RELOJERIA']):
        return 'FASHION'
    if any(x in texto for x in ['COMIDAS', 'BEBIDAS', 'RESTAURANTE', 'BAR', 'CAFETERIA', 'HOSTELERIA']):
        return 'HORECA'
    if any(x in texto for x in ['PELUQUERIA', 'ESTETICA', 'FARMACIA', 'SANITARIAS', 'GIMNASIO', 'DEPORTIVAS']):
        return 'WELLNESS'
    if any(x in texto for x in ['ALIMENTACION', 'SUPERMERCADO', 'FRUTA', 'CARNE', 'PESCADO', 'PAN']):
        return 'FOOD'

    return 'OTHER'

def process_madrid_master():
    print("🏙️ PROCESANDO MASTER CENSUS MADRID (CON GESTIÓN DE HISTÓRICO)...")
    
    # 1. BUSCAR ARCHIVO
    latest_file = get_latest_file()
    if not latest_file:
        print(f"❌ ERROR: No encuentro ningún archivo {PATTERN}")
        return
    
    print(f"   📖 Leyendo: {os.path.basename(latest_file)}...")
    
    # 2. CARGAR DATOS
    try:
        df = pd.read_csv(latest_file, sep=';', encoding='utf-8-sig', dtype=str)
    except Exception as e:
        print(f"❌ Error leyendo CSV: {e}")
        return

    # Convertir coordenadas
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df = df.dropna(subset=['lat', 'lon'])
    
    print(f"   📊 Locales válidos: {len(df):,}")

    # 3. ENRIQUECIMIENTO
    print("   ⬡ Calculando H3 y Categorías...")
    df['h3_index'] = df.apply(lambda x: h3.geo_to_h3(x['lat'], x['lon'], H3_RES), axis=1)
    df['category_group'] = df.apply(categorize_activity, axis=1)
    df['is_active'] = df['desc_situacion_local'].str.contains('Abierto', case=False, na=False).astype(int)

    # 4. GESTIÓN INTELIGENTE DEL HISTÓRICO (AQUÍ ESTÁ LA MAGIA)
    engine = create_engine(DB_URL)
    
    # Obtener fecha del snapshot actual (del archivo o del día)
    if 'snapshot_date' in df.columns:
        current_snapshot = df['snapshot_date'].iloc[0] 
    else:
        current_snapshot = pd.Timestamp.now().strftime('%Y-%m-%d')
        df['snapshot_date'] = current_snapshot

    print(f"   📅 Gestionando Snapshot: {current_snapshot}")

    with engine.connect() as conn:
        # A. Verificar si la tabla existe (para no fallar en el SELECT)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS locales_madrid_history (
                id_local TEXT,
                h3_index TEXT,
                snapshot_date DATE,
                desc_situacion_local TEXT,
                category_group TEXT,
                rotulo TEXT,
                lat FLOAT,
                lon FLOAT
            );
        """))
        
        # B. Contar si hay registros de esta misma fecha
        result = conn.execute(text(f"SELECT COUNT(*) FROM locales_madrid_history WHERE snapshot_date = '{current_snapshot}'"))
        count = result.scalar()
        
        if count > 0:
            print(f"      ⚠️ Detectados {count} registros previos de {current_snapshot}.")
            print("      🔄 Borrando versión anterior para re-emplazar (Clean Update)...")
            conn.execute(text(f"DELETE FROM locales_madrid_history WHERE snapshot_date = '{current_snapshot}'"))
            conn.commit()
        else:
            print("      ✅ Fecha nueva. Preparando inserción limpia.")

    # 5. INSERTAR (APPEND SEGURO)
    print("   💾 Insertando datos en 'locales_madrid_history'...")
    
    # Columnas a guardar
    cols_to_save = ['id_local', 'h3_index', 'snapshot_date', 'desc_situacion_local', 'category_group', 'rotulo', 'lat', 'lon']
    # Filtramos solo las que existen por si acaso
    final_cols = [c for c in cols_to_save if c in df.columns]
    
    df_hist = df[final_cols].copy()
    
    # Usamos chunksize para no saturar la memoria
    df_hist.to_sql('locales_madrid_history', engine, if_exists='append', index=False, chunksize=10000)

    # 6. AGREGACIÓN Y UPDATE DE KPI (ENRICHED)
    print("   ∑ Actualizando KPIs en tabla maestra...")
    
    stats = df.groupby('h3_index').agg(
        total_locales=('id_local', 'count'),
        active_locales=('is_active', 'sum'),
        cnt_fashion=('category_group', lambda x: (x == 'FASHION').sum()),
        cnt_horeca=('category_group', lambda x: (x == 'HORECA').sum())
    ).reset_index()
    
    stats['vacancy_rate'] = 1 - (stats['active_locales'] / stats['total_locales'])
    
    stats.to_sql('temp_madrid_kpis', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Asegurar columnas
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS comm_density INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS vacancy_rate FLOAT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS fashion_count INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS horeca_count INT DEFAULT 0;"))
        
        # Update
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET comm_density = s.total_locales,
                vacancy_rate = s.vacancy_rate,
                fashion_count = s.cnt_fashion,
                horeca_count = s.cnt_horeca
            FROM temp_madrid_kpis AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_madrid_kpis;"))
        conn.commit()

    print("✅ PROCESO COMPLETADO. Histórico limpio y KPIs actualizados.")

if __name__ == "__main__":
    process_madrid_master()
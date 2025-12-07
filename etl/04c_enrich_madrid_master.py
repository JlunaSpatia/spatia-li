import pandas as pd
from sqlalchemy import create_engine, text
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

warnings.filterwarnings("ignore")

def update_enriched_layer():
    print("🔄 ACTUALIZANDO CAPA MAESTRA (Vía Spatial Join PostGIS)...")
    engine = create_engine(DB_URL)

    # 1. OBTENER EL ÚLTIMO SNAPSHOT
    print("    📅 Buscando la fecha más reciente en 'locales_madrid_history'...")
    try:
        query_date = "SELECT MAX(snapshot_date) FROM locales_madrid_history"
        latest_date = pd.read_sql(query_date, engine).iloc[0, 0]
        
        if not latest_date:
            print("❌ ERROR: La tabla histórica está vacía.")
            return
            
        print(f"       -> Último snapshot: {latest_date}")
    except Exception as e:
        print(f"❌ Error conectando a BBDD: {e}")
        return

    # 2. SPATIAL JOIN (CRUCE ESPACIAL EN BBDD)
    # Aquí ocurre la magia: Cruzamos puntos (L) con polígonos (H) usando ST_Intersects
    print("    ⚔️  Ejecutando cruce espacial (Puntos vs Hexágonos)...")
    
    query_spatial_join = f"""
        SELECT 
            h.h3_index,                 -- El ID viene del hexágono donde cae el punto
            l.id_local, 
            l.desc_situacion_local, 
            l.category_group 
        FROM locales_madrid_history l
        JOIN retail_hexagons_enriched h
          ON ST_Intersects(l.geometry, h.geometry) -- Condición espacial
        WHERE l.snapshot_date = '{latest_date}'
    """
    
    try:
        df = pd.read_sql(query_spatial_join, engine)
    except Exception as e:
        print(f"    ❌ Error en el cruce espacial: {e}")
        print("       SUGERENCIA: Asegúrate de que ambas tablas tienen columna 'geometry' y el mismo SRID (4326).")
        return
    
    if df.empty:
        print("    ⚠️ ALERTA: El cruce espacial no devolvió resultados.")
        print("       Verifica que los hexágonos cubran la zona de los puntos.")
        return

    print(f"       -> {len(df):,} locales han sido asignados a un hexágono.")

    # 3. RECALCULAR KPIS
    print("    ∑  Recalculando métricas...")
    
    # Normalizar estado (Abierto = 1)
    df['is_active'] = df['desc_situacion_local'].astype(str).str.contains('Abierto', case=False, na=False).astype(int)
    
    # Agrupación por el h3_index que hemos recuperado del cruce
    stats = df.groupby('h3_index').agg(
        total_locales=('id_local', 'count'),
        active_locales=('is_active', 'sum'),
        cnt_fashion=('category_group', lambda x: (x == 'FASHION').sum()),
        cnt_horeca=('category_group', lambda x: (x == 'HORECA').sum()),
        cnt_wellness=('category_group', lambda x: (x == 'WELLNESS').sum())
    ).reset_index()
    
    # Ratios
    stats['vacancy_rate'] = 0.0
    mask_vals = stats['total_locales'] > 0
    stats.loc[mask_vals, 'vacancy_rate'] = 1 - (stats.loc[mask_vals, 'active_locales'] / stats.loc[mask_vals, 'total_locales'])
    
    print(f"       -> KPIs listos para {len(stats):,} hexágonos.")

    # 4. ACTUALIZAR BBDD
    print("    💾 Volcando datos a 'retail_hexagons_enriched'...")
    
    stats.to_sql('temp_kpi_update', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Añadimos columnas si faltan
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS comm_density INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS vacancy_rate FLOAT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS fashion_count INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS horeca_count INT DEFAULT 0;"))
        conn.commit()

        # Reseteamos valores a 0 antes de actualizar (opcional, para limpiar zonas que ahora no tienen locales)
        # conn.execute(text("UPDATE retail_hexagons_enriched SET comm_density=0, vacancy_rate=0, fashion_count=0, horeca_count=0;"))

        print("       -> Ejecutando UPDATE masivo...")
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET comm_density = s.total_locales,
                vacancy_rate = s.vacancy_rate,
                fashion_count = s.cnt_fashion,
                horeca_count = s.cnt_horeca
            FROM temp_kpi_update AS s
            WHERE m.h3_index = s.h3_index;
        """))
        
        conn.execute(text("DROP TABLE temp_kpi_update;"))
        conn.commit()

    print("✅ PROCESO COMPLETADO. Capa sincronizada mediante cruce espacial.")
    
    top_dens = stats.sort_values('total_locales', ascending=False).head(1)
    if not top_dens.empty:
        print(f"    🏆 Zona más densa: {top_dens.iloc[0]['h3_index']} ({top_dens.iloc[0]['total_locales']} locales)")

if __name__ == "__main__":
    update_enriched_layer()
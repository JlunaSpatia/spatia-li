import pandas as pd
from sqlalchemy import create_engine, text
import warnings

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
warnings.filterwarnings("ignore")

def update_madrid_master_layer():
    print("🇪🇸 INICIANDO PROCESO MAESTRO MADRID (04c)...")
    engine = create_engine(DB_URL)

    # =========================================================
    # PARTE 1: CÁLCULO DE MÉTRICAS (SPATIAL JOIN)
    # =========================================================
    print("🔄 1. OBTENIENDO MÉTRICAS REALES DE LOCALES...")
    
    # 1.1 Obtener fecha del último censo disponible
    try:
        query_date = "SELECT MAX(snapshot_date) FROM locales_madrid_history"
        latest_date = pd.read_sql(query_date, engine).iloc[0, 0]
        if not latest_date:
            print("❌ Error: Tabla histórica vacía.")
            return
        print(f"       -> Usando Snapshot: {latest_date}")
    except Exception as e:
        print(f"❌ Error conectando a DB: {e}")
        return

    # 1.2 Spatial Join (PostGIS)
    # Cruzamos Puntos (Locales) con Polígonos (Hexágonos de Madrid)
    print("       -> Ejecutando cruce espacial (ST_Intersects)...")
    query_spatial_join = f"""
        SELECT 
            h.h3_index, 
            l.desc_situacion_local, 
            l.category_group 
        FROM locales_madrid_history l
        JOIN retail_hexagons_enriched h
          ON ST_Intersects(l.geometry, h.geometry)
        WHERE l.snapshot_date = '{latest_date}'
          AND h.city = 'Madrid' -- Filtro de seguridad
    """
    
    df = pd.read_sql(query_spatial_join, engine)
    
    if df.empty:
        print("⚠️ ALERTA: No se encontraron locales dentro de los hexágonos.")
        return

    # 1.3 Recalcular KPIs en Python
    print(f"       -> Procesando {len(df):,} locales encontrados...")
    
    # Normalizar estado (Abierto = 1, Cerrado/Baja = 0)
    df['is_active'] = df['desc_situacion_local'].astype(str).str.contains('Abierto', case=False, na=False).astype(int)
    
    stats = df.groupby('h3_index').agg(
        comm_density=('category_group', 'count'),      # Total locales (Volumen)
        active_locales=('is_active', 'sum'),           # Locales abiertos
        fashion_count=('category_group', lambda x: (x == 'FASHION').sum()),
        horeca_count=('category_group', lambda x: (x == 'HORECA').sum())
    ).reset_index()
    
    # --- CÁLCULO DE SALUD (OCCUPANCY RATE) ---
    # 1.0 = 100% Ocupado (Éxito) | 0.0 = Todo Cerrado (Fantasma)
    stats['vacancy_rate'] = stats['active_locales'] / stats['comm_density']

    # 1.4 Volcar métricas numéricas a DB
    print("       -> Guardando métricas crudas en BBDD...")
    stats.to_sql('temp_kpi_update', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        # Crear columnas si no existen
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS comm_density INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS vacancy_rate FLOAT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS fashion_count INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS horeca_count INT DEFAULT 0;"))
        
        # Update masivo desde tabla temporal
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET comm_density = s.comm_density,
                vacancy_rate = s.vacancy_rate,
                fashion_count = s.fashion_count,
                horeca_count = s.horeca_count
            FROM temp_kpi_update AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_kpi_update;"))
        conn.commit()

    # =========================================================
    # PARTE 2: ETIQUETADO INTELIGENTE (SCORING)
    # =========================================================
    print("🏷️  2. GENERANDO ETIQUETAS DE NEGOCIO (VIBE & RISK)...")
    
    # Definimos la Masa Crítica para evitar juzgar "Millas de Oro emergentes" incorrectamente
    MIN_CRITICAL_MASS = 5 

    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS street_profile TEXT;"))
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS health_status TEXT;"))
        
        # Update Lógico con SQL Case
        sql_profile = f"""
        UPDATE retail_hexagons_enriched
        SET 
            -- A. PERFIL DE LA CALLE (VIBE)
            street_profile = CASE 
                WHEN fashion_count > horeca_count THEN 'Fashion District'
                WHEN horeca_count > fashion_count THEN 'Food & Beverage Zone'
                WHEN comm_density > 10 AND (fashion_count + horeca_count) < 2 THEN 'Service/Misc'
                ELSE 'Residential/Mixed'
            END,
            
            -- B. ESTADO DE SALUD (RISK MATRIX)
            health_status = CASE 
                -- 1. Sin datos comerciales
                WHEN comm_density = 0 THEN 'No Data'
                
                -- 2. Zonas Emergentes (Poca densidad, no es justo penalizarlas)
                WHEN comm_density < {MIN_CRITICAL_MASS} THEN 
                    CASE 
                        WHEN vacancy_rate >= 0.8 THEN 'Emerging / Niche' -- Pocos locales, pero funcionan (Oportunidad)
                        ELSE 'Sparse / Residential' -- Pocos locales y cerrados (Sin interés)
                    END

                -- 3. Zonas Consolidadas (Masa Crítica suficiente para juzgar)
                WHEN comm_density >= {MIN_CRITICAL_MASS} THEN
                    CASE
                        WHEN vacancy_rate >= 0.90 THEN 'Prime (Low Risk)'
                        WHEN vacancy_rate BETWEEN 0.60 AND 0.89 THEN 'Standard'
                        WHEN vacancy_rate < 0.60 THEN 'Distressed (High Risk)' -- Zona comercial muriendo
                    END
                
                ELSE 'Unknown'
            END
        WHERE CITY = 'Madrid'; -- Aplicar solo a Madrid
        """
        
        conn.execute(text(sql_profile))
        conn.commit()

    print("✅ MASTER DATA INTEGRADO. Hexágonos categorizados correctamente.")

if __name__ == "__main__":
    update_madrid_master_layer()
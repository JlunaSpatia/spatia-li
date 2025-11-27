import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN ---
# Tus credenciales del docker-compose.yml
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "spatia"

# Cadena de conexión
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_data_to_postgis():
    print("💾 Leyendo CSV generado...")
    try:
        df = pd.read_csv("etl/final_dataset.csv")
    except FileNotFoundError:
        print("❌ Error: No encuentro 'etl/final_dataset.csv'. Ejecuta primero el paso 01.")
        return

    print(f"CONNECTING to PostGIS ({DB_NAME})...")
    engine = create_engine(DATABASE_URL)

    # 1. Subir la tabla tal cual (como datos planos)
    print("🚀 Subiendo datos a la tabla 'retail_hexagons'...")
    df.to_sql('retail_hexagons', engine, if_exists='replace', index=False)
    
    # 2. La Magia de PostGIS: Crear columna de Geometría
    # Ahora mismo lat/lon son números. Vamos a convertirlos en puntos espaciales reales.
    print("🌍 Georreferenciando la tabla (Creating Geometry Column)...")
    
    with engine.connect() as conn:
        # Añadimos columna geométrica
        conn.execute(text("ALTER TABLE retail_hexagons ADD COLUMN geom geometry(Point, 4326);"))
        
        # Llenamos la columna geométrica usando lat/lon
        conn.execute(text("UPDATE retail_hexagons SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);"))
        
        # Creamos un índice espacial (Esto hace que las consultas sean milisegundos en lugar de segundos)
        conn.execute(text("CREATE INDEX idx_retail_hexagons_geom ON retail_hexagons USING GIST(geom);"))
        
        conn.commit()

    print("✅ ¡ÉXITO! Datos cargados en PostGIS.")
    print("   Ahora puedes abrir QGIS y conectar a tu base de datos para ver los puntos.")

if __name__ == "__main__":
    load_data_to_postgis()
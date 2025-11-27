import pandas as pd
from sqlalchemy import create_engine
import h3
from shapely.geometry import Polygon
import geopandas as gpd

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

def load_data_to_postgis():
    print("💾 Leyendo CSV generado...")
    try:
        df = pd.read_csv("etl/final_dataset.csv")
    except FileNotFoundError:
        print("❌ No encuentro el CSV.")
        return

    # --- CAMBIO CLAVE: GENERAR POLÍGONOS ---
    print("⬡ Convirtiendo índices H3 a Polígonos Reales...")
    
    def get_hex_polygon(h3_index):
        # Obtiene los vértices del hexágono
        boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        # OJO: h3 devuelve (lat, lon), shapely espera (lon, lat). A veces hay que invertir.
        # h3-py v3 suele devolver (lng, lat) si geo_json=True. Verificamos visualmente luego.
        return Polygon(boundary)

    # Creamos columna de geometría real
    df['geometry'] = df['h3_index'].apply(get_hex_polygon)
    
    # Convertimos a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.set_crs(epsg=4326, inplace=True) # WGS84

    print(f"CONNECTING to PostGIS...")
    engine = create_engine(DB_URL)

    # Usamos GeoPandas para subirlo directamente como geometría (es más limpio que SQL manual)
    print("🚀 Subiendo tabla 'retail_hexagons' con geometría POLYGON...")
    gdf.to_postgis('retail_hexagons', engine, if_exists='replace', index=False)
    
    print("✅ ¡ÉXITO! Ahora en QGIS verás hexágonos, no puntos.")

if __name__ == "__main__":
    load_data_to_postgis()
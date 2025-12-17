import os
import sys
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text

# ================= SETUP ROBUSTO DE RUTAS =================
script_path = os.path.abspath(__file__)
market_scanner_dir = os.path.dirname(script_path)
project_root = os.path.dirname(market_scanner_dir)

# Añadimos rutas al sistema para encontrar config.py
if market_scanner_dir not in sys.path: sys.path.insert(0, market_scanner_dir)
if project_root not in sys.path: sys.path.append(project_root)

try:
    import config
    from config import DB_CONNECTION_STR
    print("✅ Configuración cargada.")
except ImportError:
    sys.exit("❌ Error: No encuentro config.py")

# ================= FUNCIONES =================

def export_city_to_shp(city_name):
    """
    1. Conecta a PostGIS.
    2. Descarga todos los POIs de la ciudad indicada.
    3. Los convierte a GeoDataFrame.
    4. Exporta a Shapefile (.shp).
    """
    print(f"\n📦 INICIANDO EXPORTACIÓN PARA: {city_name}")
    
    # 1. Definir Ruta de Salida
    # Guardaremos en: project_root/data/processed/shapefiles/
    output_dir = os.path.join(project_root, "data", "processed", "shapefiles")
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{city_name}_POIS.shp")

    # 2. Consultar Base de Datos
    print("🛢️  Leyendo datos de PostGIS...")
    engine = create_engine(DB_CONNECTION_STR)
    
    # NOTA: Ajusta 'retail_poi_master' si tu tabla se llama diferente
    query = f"""
        SELECT * FROM public.retail_poi_master 
        WHERE UPPER(city) = UPPER('{city_name}')
    """
    
    try:
        # Usamos pandas primero para leer SQL
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print(f"⚠️  No se encontraron datos para la ciudad: {city_name}")
            return
        
        print(f"   -> {len(df)} puntos encontrados.")

        # 3. Convertir a GeoDataFrame
        # Si ya tienes columna 'geometry' en formato binario, usa gpd.read_postgis.
        # Si tienes lat/lon sueltos, usamos esto:
        if 'geometry' not in df.columns and 'longitude' in df.columns:
            print("🗺️  Construyendo geometrías desde Lat/Lon...")
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df.longitude, df.latitude),
                crs="EPSG:4326"
            )
        else:
            # Si ya vino con geometría pero como texto o binario, hay que asegurarse
            # Asumimos que PostGIS devuelve lat/lon limpios por seguridad
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df.longitude, df.latitude),
                crs="EPSG:4326"
            )

        # 4. Limpieza para Shapefile (Limitaciones del formato)
        # Los Shapefiles odian las fechas (datetime) y nombres de columna largos (>10 caracteres)
        
        # Convertir timestamps a strings
        for col in gdf.columns:
            if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                gdf[col] = gdf[col].astype(str)

        # 5. Guardar
        print(f"💾 Guardando Shapefile en: {output_file}")
        gdf.to_file(output_file, driver='ESRI Shapefile', encoding='utf-8')
        
        print(f"✅ ¡ÉXITO! Archivos generados en {output_dir}")
        print("   (Recuerda que un Shapefile son 3 o 4 archivos: .shp, .shx, .dbf, .prj)")

    except Exception as e:
        print(f"❌ Error durante la exportación: {e}")

if __name__ == "__main__":
    # CAMBIA AQUÍ LA CIUDAD QUE QUIERAS EXPORTAR
    export_city_to_shp("BARCELONA")
    # export_city_to_shp("ALCALA")
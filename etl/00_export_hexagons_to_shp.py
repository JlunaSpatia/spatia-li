import os
import sys
import geopandas as gpd
from sqlalchemy import create_engine

# ================= SETUP DE RUTAS =================
# Al estar en 'etl/', el root del proyecto es la carpeta padre
script_path = os.path.abspath(__file__)
etl_dir = os.path.dirname(script_path)
project_root = os.path.dirname(etl_dir)

# Añadimos el root al path para poder importar config.py
if project_root not in sys.path: sys.path.append(project_root)

try:
    import config
    from config import DB_CONNECTION_STR, ACTIVE_CITIES
    print("✅ Configuración cargada correctamente desde el root.")
except ImportError:
    sys.exit("❌ Error: No encuentro config.py. Verifica la estructura de carpetas.")

# ================= FUNCIÓN PRINCIPAL =================

def export_hexagons():
    # Definir dónde guardar los archivos (carpeta data/processed/shapefiles)
    output_dir = os.path.join(project_root, "data", "processed", "shapefiles")
    os.makedirs(output_dir, exist_ok=True)

    # Conexión a la BBDD
    engine = create_engine(DB_CONNECTION_STR)

    if not ACTIVE_CITIES:
        print("⚠️ La lista ACTIVE_CITIES en config.py está vacía.")
        return

    for city in ACTIVE_CITIES:
        print(f"\n🏗️  Procesando ciudad: {city}...")
        
        # 1. Query SQL directa a PostGIS
        # Recuperamos todo de la tabla retail_hexagons para esa ciudad
        sql = f"""
            SELECT * FROM public.retail_hexagons 
            WHERE UPPER(city) = UPPER('{city}')
        """
        
        try:
            print(f"   ⏳ Consultando PostGIS para {city}...")
            # read_postgis detecta automáticamente la columna de geometría
            gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
            
            if gdf.empty:
                print(f"   ⚠️ No se encontraron datos para {city}. ¿Has ejecutado el cálculo de hexágonos antes?")
                continue

            print(f"   ✅ {len(gdf)} hexágonos descargados.")

            # 2. Limpieza para formato Shapefile (ESRI)
            # Los Shapefiles son antiguos y muy estrictos:
            # - No soportan valores nulos en algunos campos numéricos.
            # - No soportan listas o diccionarios (JSON).
            # - Los nombres de columna se cortan a 10 caracteres.
            
            # Convertimos columnas complejas a string para evitar errores
            for col in gdf.columns:
                if col != 'geometry':
                    # Si es objeto (texto, lista, etc) forzamos string
                    if gdf[col].dtype == 'object':
                        gdf[col] = gdf[col].astype(str)
                        # Reemplazar 'nan' string por vacío si prefieres
                        gdf[col] = gdf[col].replace('nan', '')

            # 3. Exportar a Shapefile
            # Limpiamos el nombre de archivo para evitar espacios
            safe_city_name = city.replace(" ", "_")
            filename = f"{safe_city_name}_HEXAGONS.shp"
            filepath = os.path.join(output_dir, filename)
            
            print(f"   💾 Generando Shapefile en: {filepath}")
            gdf.to_file(filepath, driver='ESRI Shapefile', encoding='utf-8')
            print("   ✨ ¡Exportación completada!")

        except Exception as e:
            print(f"   ❌ Error exportando {city}: {e}")

if __name__ == "__main__":
    export_hexagons()
import pandas as pd
import geopandas as gpd
import numpy as np
import os

# --- CONFIGURACIÓN ---
INPUT_LOCALES = "data/raw/OPEN_DATA_LOCALES_MADRID_WGS84.csv"
INPUT_ACTIVIDAD = "data/raw/OPEN_DATA_ACTIVIDAD_MADRID_WGS84.csv"
OUT_DIR = "data/export_shp"

def export_shp(csv_path, filename_base):
    print(f"🌍 Convirtiendo {filename_base} a Shapefile...")
    
    if not os.path.exists(csv_path):
        print(f"   ⚠️ No encuentro {csv_path}.")
        return

    try:
        # 1. Cargar CSV
        # low_memory=False ayuda con las columnas mixtas
        df = pd.read_csv(csv_path, low_memory=False)
        
        # 2. Limpieza de Coordenadas (EL FIX)
        # Aseguramos que sean números
        df['longitud_wgs84'] = pd.to_numeric(df['longitud_wgs84'], errors='coerce')
        df['latitud_wgs84'] = pd.to_numeric(df['latitud_wgs84'], errors='coerce')
        
        # Eliminamos NaNs
        df = df.dropna(subset=['longitud_wgs84', 'latitud_wgs84'])
        
        # Eliminamos Infinitos (el error que te daba)
        df = df[np.isfinite(df['longitud_wgs84']) & np.isfinite(df['latitud_wgs84'])]
        
        print(f"   -> Filas válidas: {len(df)}")

        # 3. GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['longitud_wgs84'], df['latitud_wgs84']),
            crs="EPSG:4326"
        )

        # 4. Limpieza de Columnas para Shapefile (Opcional pero recomendado)
        # Shapefile no soporta Datetimes, convertimos todo a string para evitar líos
        for col in gdf.columns:
            if col != 'geometry':
                gdf[col] = gdf[col].astype(str)

        # 5. Guardar
        if not os.path.exists(OUT_DIR):
            os.makedirs(OUT_DIR)
            
        output_path = os.path.join(OUT_DIR, f"{filename_base}.shp")
        gdf.to_file(output_path, driver="ESRI Shapefile", encoding='utf-8')
        
        print(f"   ✅ Guardado en {output_path}")

    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    export_shp(INPUT_LOCALES, "madrid_locales")
    export_shp(INPUT_ACTIVIDAD, "madrid_actividad")
    print("\n🎉 Proceso terminado. Abre QGIS.")
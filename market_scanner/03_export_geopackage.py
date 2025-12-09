import pandas as pd
import geopandas as gpd
import os
import fiona

# --- CONFIGURACIÓN ---
QUARTER = "2025_Q4"

def export_robust_gpkg(city_name):
    # 1. Rutas
    base_dir = os.path.join("data", "processed", QUARTER)
    input_csv = os.path.join(base_dir, f"{city_name}_MASTER.csv")
    output_gpkg = os.path.join(base_dir, f"{city_name}_RETAIL_GENOME.gpkg")

    print(f"🌍 GENERANDO GEOPACKAGE ROBUSTO: {city_name}")
    
    # Check Drivers
    if 'GPKG' not in fiona.supported_drivers:
        print("❌ CRÍTICO: Tu instalación de Python no tiene el driver 'GPKG'.")
        print("   Intenta instalar: pip install fiona --upgrade")
        return

    if not os.path.exists(input_csv):
        print(f"❌ Error: No encuentro {input_csv}")
        return

    # 2. Cargar Datos
    df = pd.read_csv(input_csv, low_memory=False) # low_memory=False ayuda con tipos mixtos
    print(f"   📥 Leídos {len(df)} registros.")

    # 3. LIMPIEZA AGRESIVA (SANITIZACIÓN)
    # Eliminar filas sin coordenadas
    df = df.dropna(subset=['lat', 'lon'])
    
    # Renombrar para evitar espacios o caracteres raros
    rename_map = {
        'standardized_status': 'status',
        'title': 'nombre',
        'type': 'categoria',
        'address': 'direccion'
    }
    df.rename(columns=rename_map, inplace=True)

    # ⚠️ TRUCO DEL ALMENDRUCO:
    # Convertir TODAS las columnas (menos lat/lon/rating/reviews) a String explícito.
    # Esto evita que QGIS rechace el archivo por tipos mixtos.
    for col in df.columns:
        if col not in ['lat', 'lon', 'rating', 'reviews']:
            df[col] = df[col].astype(str).fillna("")

    # Asegurar numéricos
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
    df['reviews'] = pd.to_numeric(df['reviews'], errors='coerce').fillna(0)

    # 4. Georreferenciación
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326"
    )

    # 5. Exportar
    try:
        if os.path.exists(output_gpkg):
            os.remove(output_gpkg) # Borrar anterior para evitar conflictos de bloqueo

        gdf.to_file(output_gpkg, driver='GPKG', layer=city_name)
        
        file_size = os.path.getsize(output_gpkg) / 1024 # KB
        print(f"✅ ¡ÉXITO! GeoPackage generado ({file_size:.1f} KB).")
        print(f"   📂 {output_gpkg}")
        
    except Exception as e:
        print(f"❌ Error guardando GeoPackage: {e}")
        print("💡 INTENTO DE EMERGENCIA: Guardando como SHAPEFILE...")
        # Fallback a Shapefile si GPKG falla
        shp_path = output_gpkg.replace(".gpkg", ".shp")
        gdf.to_file(shp_path, driver='ESRI Shapefile')
        print(f"   ⚠️ Se guardó como .shp en su lugar: {shp_path}")

if __name__ == "__main__":
    export_robust_gpkg("MADRID")
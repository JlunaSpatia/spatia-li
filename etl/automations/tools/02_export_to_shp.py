import pandas as pd
import geopandas as gpd
import os
import warnings

# --- CONFIGURACIÓN ---
INPUT_MASTER = "data/raw/MADRID_MASTER_CENSUS_20251130.csv"
OUT_DIR = "data/export_shp"
FILE_NAME = "madrid_master_census_v3.shp"

warnings.filterwarnings("ignore")

def export_master_to_shp_utf8():
    print("🌍 EXPORTANDO MASTER A SHAPEFILE (FIX ENCODING)...")

    if not os.path.exists(INPUT_MASTER):
        print(f"❌ ERROR: No encuentro {INPUT_MASTER}")
        return

    try:
        # 1. LEER EL CSV CON UTF-8-SIG (CRÍTICO)
        # 'utf-8-sig' consume el BOM que Excel añade/necesita
        print("   📖 Leyendo CSV master (utf-8-sig)...")
        df = pd.read_csv(INPUT_MASTER, sep=";", encoding="utf-8-sig", dtype=str)

        # DEBUG: Comprobamos si Python lo lee bien en memoria
        # Buscamos una fila de Hostelería para ver si sale 'HOSTELERÍA' o 'HOSTELERÃA'
        sample = df[df['desc_seccion'].str.contains('HOSTEL', na=False, case=False)].head(1)
        if not sample.empty:
            print(f"   👀 Test de lectura en memoria: {sample['desc_seccion'].iloc[0]}")
            # Si aquí sale bien (con tilde), el problema de antes era solo de lectura

        # 2. CONVERTIR COORDENADAS
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])

        # 3. CREAR GEODATAFRAME
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326",
        )

        # 4. RENOMBRAR CAMPOS
        rename_map = {
            "id_local": "ID_LOCAL",
            "rotulo": "ROTULO",
            "desc_situacion_local": "SITUACION",
            "desc_barrio_local": "BARRIO",
            "desc_seccion": "ACT_SECC",
            "desc_division": "ACT_DIV",
            "desc_epigrafe": "ACT_EPIG",
            "snapshot_date": "FECHA_REF",
            "lat": "LATITUD",
            "lon": "LONGITUD",
        }
        cols_to_rename = {k: v for k, v in rename_map.items() if k in gdf.columns}
        gdf = gdf.rename(columns=cols_to_rename)

        # 5. LIMPIEZA TIPOS
        for col in gdf.columns:
            if col != "geometry":
                gdf[col] = gdf[col].fillna("").astype(str).str.slice(0, 254)

        # 6. GUARDAR (FORZANDO UTF-8 EN EL DRIVER)
        if not os.path.exists(OUT_DIR):
            os.makedirs(OUT_DIR, exist_ok=True)

        output_path = os.path.join(OUT_DIR, FILE_NAME)
        print(f"   💾 Escribiendo .shp en: {output_path}")

        # El driver 'ESRI Shapefile' de GDAL suele generar un archivo .cpg
        # que indica la codificación. Python lo hace automático con encoding='utf-8'.
        gdf.to_file(output_path, driver="ESRI Shapefile", encoding="utf-8")

        print("✅ ¡ÉXITO! Abre el archivo en QGIS.")
        print("   NOTA: Si en QGIS se ve mal, ve a Propiedades de la capa > Fuente > Codificación y pon UTF-8.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    export_master_to_shp_utf8()
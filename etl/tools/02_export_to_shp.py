import pandas as pd
import geopandas as gpd
import os
import warnings

# --- CONFIGURACIÓN ---
INPUT_MASTER = "data/raw/MADRID_MASTER_CENSUS.csv"
OUT_DIR = "data/export_shp"
FILE_NAME = "madrid_master_census_v2.shp"

warnings.filterwarnings("ignore")


def export_master_to_shp():
    print("🌍 EXPORTANDO MASTER A SHAPEFILE...")

    if not os.path.exists(INPUT_MASTER):
        print(f"❌ ERROR: No encuentro {INPUT_MASTER}")
        return

    try:
        # 1. LEER EL CSV MASTER
        print("   📖 Leyendo CSV master...")
        df = pd.read_csv(INPUT_MASTER, sep=";", encoding="utf-8")

        print("   🔎 Columnas encontradas:", list(df.columns))

        # Columnas que esperamos en el master
        expected_cols = [
            "id_local",
            "rotulo",
            "desc_situacion_local",
            "desc_barrio_local",
            "lat",
            "lon",
        ]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            print("❌ ERROR: Faltan columnas en el master:", missing)
            return

        # 2. CONVERTIR COORDENADAS A FLOAT
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

        # Filtrar posibles nulos
        df = df.dropna(subset=["lat", "lon"])

        print(f"   📊 Filas válidas a exportar: {len(df):,}")
        if len(df) == 0:
            print("⚠️ No hay filas con coordenadas válidas. Nada que exportar.")
            return

        # 3. CREAR GEODATAFRAME (ya en WGS84)
        print("   📍 Generando geometría (EPSG:4326)...")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326",
        )

        # 4. RENOMBRAR CAMPOS A ≤10 CARACTERES PARA SHAPEFILE
        rename_map = {
            "id_local": "ID_LOCAL",
            "rotulo": "ROTULO",
            "desc_situacion_local": "SIT_LOCAL",
            "desc_barrio_local": "BARRIO_LOC",
            "lat": "LAT",
            "lon": "LON",
        }
        gdf = gdf.rename(columns=rename_map)

        # 5. FORZAR TIPOS: LAT/LON NUMÉRICOS, RESTO TEXTO
        for col in gdf.columns:
            if col in ("LAT", "LON", "geometry"):
                continue
            gdf[col] = gdf[col].fillna("").astype(str).str.slice(0, 254)

        # 6. CREAR CARPETA Y GUARDAR SHAPEFILE
        if not os.path.exists(OUT_DIR):
            os.makedirs(OUT_DIR, exist_ok=True)

        output_path = os.path.join(OUT_DIR, FILE_NAME)
        print(f"   💾 Escribiendo .shp en: {output_path}")

        gdf.to_file(output_path, driver="ESRI Shapefile", encoding="utf-8")

        print("✅ ¡ÉXITO! Abre el shapefile en ArcMap/QGIS (CRS: EPSG:4326).")

    except Exception as e:
        print(f"❌ Error crítico exportando: {e}")


if __name__ == "__main__":
    export_master_to_shp()

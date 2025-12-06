import pandas as pd
import geopandas as gpd
import numpy as np
import os
import warnings
import logging
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN ---
FILE_LOCALES_RAW = "data/raw/locales202512.csv"
FILE_ACTIVIDAD_RAW = "data/raw/actividadeconomica202512.csv"
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# Proyecciones
CRS_ORIGEN = "EPSG:25830"
CRS_DESTINO = "EPSG:4326"

# Descomentar para debug profundo de SQL
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

def clean_number_madrid(val):
    if pd.isna(val) or str(val).strip() == "": return np.nan
    s = str(val).strip()
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try: return float(s)
    except: return np.nan

def read_smart_csv(path):
    if not os.path.exists(path): return None
    attempts = [
        (";", "utf-8"), 
        (";", "latin-1"), 
        (",", "utf-8"), 
        (",", "latin-1")
    ]
    for sep, enc in attempts:
        try:
            df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, on_bad_lines="skip")
            if len(df.columns) > 1: 
                print(f"    ✅ Leído correctamente con encoding: {enc}")
                return df
        except: continue
    return None

def prepare_coordinates(df, tag):
    cx_loc = next((c for c in df.columns if "coordenada_x_local" in c.lower()), None)
    cy_loc = next((c for c in df.columns if "coordenada_y_local" in c.lower()), None)
    cx_agr = next((c for c in df.columns if "coordenada_x_agrup" in c.lower()), None)
    cy_agr = next((c for c in df.columns if "coordenada_y_agrup" in c.lower()), None)

    if not cx_loc or not cy_loc: return df

    df["x_loc"] = df[cx_loc].apply(clean_number_madrid)
    df["y_loc"] = df[cy_loc].apply(clean_number_madrid)
    
    if cx_agr and cy_agr:
        df["x_agr"] = df[cx_agr].apply(clean_number_madrid)
        df["y_agr"] = df[cy_agr].apply(clean_number_madrid)
    else:
        df["x_agr"], df["y_agr"] = np.nan, np.nan

    valid_loc = (df["x_loc"] > 1000) & (df["y_loc"] > 1000)
    df[f"BEST_X_{tag}"] = np.where(valid_loc, df["x_loc"], df["x_agr"])
    df[f"BEST_Y_{tag}"] = np.where(valid_loc, df["y_loc"], df["y_agr"])
    return df

def upload_to_postgis_history(df):
    """
    Sube datos verificando duplicados por snapshot_date.
    Reglas:
    1. Si snapshot no existe -> INSERT.
    2. Si snapshot existe y count es igual -> SKIP (Ya está subido).
    3. Si snapshot existe y count es distinto -> WARNING (Posible error de datos).
    """
    print("    🗄️  Verificando estado en PostGIS...")
    engine = create_engine(DB_URL)
    table_name = 'locales_madrid_history'
    
    # Obtenemos el snapshot actual del dataframe
    if 'snapshot_date' not in df.columns:
        print("    ❌ Error: El DataFrame no tiene la columna 'snapshot_date'.")
        return

    current_snapshot = df['snapshot_date'].iloc[0]
    new_rows_count = len(df)
    
    # Convertimos a GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df['lon'], df['lat']),
        crs="EPSG:4326"
    )

    db_rows_count = 0
    table_exists = False

    # 1. VERIFICACIÓN CONTRA BBDD
    try:
        with engine.connect() as conn:
            # Verificar si existe la tabla
            exists_query = text(f"SELECT to_regclass('public.{table_name}');")
            if conn.execute(exists_query).scalar():
                table_exists = True
                
                # Verificar si existe ESTE snapshot y cuántas filas tiene
                count_query = text(f"SELECT count(*) FROM public.{table_name} WHERE snapshot_date = :snap")
                db_rows_count = conn.execute(count_query, {"snap": current_snapshot}).scalar()
            else:
                print(f"    ✨ La tabla '{table_name}' no existe. Se creará desde cero.")

    except Exception as e:
        print(f"    ⚠️ Error conectando para verificar: {e}")
        return

    # 2. LÓGICA DE NEGOCIO
    if table_exists and db_rows_count > 0:
        if db_rows_count == new_rows_count:
            print(f"    🛑 INFO: El snapshot {current_snapshot} ya existe con {db_rows_count} filas.")
            print("       -> No se realizará ninguna acción (SKIP).")
            return
        else:
            print(f"    ⚠️ WARNING: El snapshot {current_snapshot} YA EXISTE en BBDD pero los números no cuadran.")
            print(f"       -> Filas en BBDD: {db_rows_count} vs Filas Nuevas: {new_rows_count}")
            print("       -> 🛡️ POR SEGURIDAD NO SE SUBIRÁ NADA. Revisa si es una carga parcial.")
            return

    # 3. SUBIDA (Solo si no existe o la tabla es nueva)
    print(f"    🚀 Subiendo NUEVO snapshot ({current_snapshot}) con {new_rows_count} filas...")
    try:
        gdf.to_postgis(table_name, engine, if_exists='append', index=False, chunksize=5000)
        print(f"    ✅ Carga completada exitosamente.")
    except Exception as e:
        print(f"    ❌ Error CRÍTICO subiendo datos: {e}")


def run_master_pipeline():
    print("🚀 INICIANDO GENERACIÓN MASTER CENSUS...")

    # 1. CARGA
    print("    📖 Leyendo Locales...")
    df_loc = read_smart_csv(FILE_LOCALES_RAW)
    print("    📖 Leyendo Actividad...")
    df_act = read_smart_csv(FILE_ACTIVIDAD_RAW)
    
    if df_loc is None: return

    col_id_loc = next(c for c in df_loc.columns if "id_local" in c.lower())
    df_loc.rename(columns={col_id_loc: "id_local"}, inplace=True)
    
    # 2. FECHA DE CARGA
    col_fx = next((c for c in df_loc.columns if "fx_carga" in c.lower() or "fecha" in c.lower()), None)
    
    if col_fx:
        raw_date = df_loc[col_fx].dropna().iloc[0]
        try:
            dt = pd.to_datetime(raw_date, dayfirst=True)
            snapshot_str = dt.strftime("%Y%m%d")
            snapshot_date = dt.strftime("%Y-%m-%d")
        except:
            snapshot_str = pd.Timestamp.now().strftime("%Y%m%d")
            snapshot_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    else:
        snapshot_str = pd.Timestamp.now().strftime("%Y%m%d")
        snapshot_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    print(f"    📅 Fecha Snapshot detectada: {snapshot_date}")

    # 3. PROCESAR COORDENADAS
    if df_act is not None:
        col_id_act = next(c for c in df_act.columns if "id_local" in c.lower())
        df_act.rename(columns={col_id_act: "id_local"}, inplace=True)
        df_act = prepare_coordinates(df_act, "ACT")
        
        act_cols = ["id_local", "BEST_X_ACT", "BEST_Y_ACT"]
        for c in ["desc_seccion", "desc_division"]:
            if c in df_act.columns: act_cols.append(c)
            
        act_lookup = df_act[df_act["BEST_X_ACT"] > 1000][act_cols].drop_duplicates("id_local")
    else:
        act_lookup = pd.DataFrame(columns=["id_local", "BEST_X_ACT", "BEST_Y_ACT"])

    df_loc = prepare_coordinates(df_loc, "LOC")
    
    print("    🚑 Cruzando ficheros...")
    merged = df_loc.merge(act_lookup, on="id_local", how="left", suffixes=("", "_act"))

    loc_ok = merged["BEST_X_LOC"] > 1000
    merged["FINAL_X"] = np.where(loc_ok, merged["BEST_X_LOC"], merged["BEST_X_ACT"])
    merged["FINAL_Y"] = np.where(loc_ok, merged["BEST_Y_LOC"], merged["BEST_Y_ACT"])

    # Filtro geográfico Madrid
    df_geo = merged[
        (merged["FINAL_X"].between(350000, 550000)) & 
        (merged["FINAL_Y"].between(4400000, 4500000))
    ].copy()

    # 4. PROYECCIÓN WGS84
    print("    🌍 Proyectando a WGS84...")
    gdf = gpd.GeoDataFrame(
        df_geo, 
        geometry=gpd.points_from_xy(df_geo["FINAL_X"], df_geo["FINAL_Y"]), 
        crs=CRS_ORIGEN
    )
    gdf = gdf.to_crs(CRS_DESTINO)
    df_geo["lon"], df_geo["lat"] = gdf.geometry.x, gdf.geometry.y

    # 5. GESTIÓN COLUMNAS
    if "desc_seccion_act" in df_geo.columns:
        df_geo["desc_seccion"] = df_geo["desc_seccion_act"].fillna(df_geo.get("desc_seccion", ""))
    if "desc_division_act" in df_geo.columns:
        df_geo["desc_division"] = df_geo["desc_division_act"].fillna(df_geo.get("desc_division", ""))

    df_geo['snapshot_date'] = snapshot_date

    # 6. GUARDAR MASTER CSV
    output_filename = f"data/raw/MADRID_MASTER_CENSUS_{snapshot_str}.csv"
    
    keep_cols = [
        "snapshot_date", "id_local", "rotulo", "desc_situacion_local", 
        "desc_seccion", "desc_division", "desc_barrio_local", 
        "lat", "lon"
    ]
    final_cols = [c for c in keep_cols if c in df_geo.columns]
    
    print(f"    💾 Guardando CSV local: {output_filename}")
    df_geo[final_cols].to_csv(output_filename, index=False, sep=";", encoding="utf-8-sig")

    # 7. SUBIR A POSTGIS (Con lógica inteligente)
    upload_to_postgis_history(df_geo[final_cols])

if __name__ == "__main__":
    run_master_pipeline()
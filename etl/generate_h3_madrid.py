import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, Point
import h3
from sqlalchemy import create_engine

# -----------------------------
# CONFIG
# -----------------------------
PATH_SHP = "data/raw/BARRIOS.shp"
PG_CONN = "postgresql://postgres:postgres@localhost:5432/spatia"
RES_LIST = [7, 8]  # 9 = coche, 10 = pie

# -----------------------------
# 1. PREPARACIÓN DE DATOS
# -----------------------------
print("Cargando shapefile…")
gdf = gpd.read_file(PATH_SHP)

if gdf.crs != "EPSG:4326":
    print("Reproyectando a EPSG:4326…")
    gdf = gdf.to_crs("EPSG:4326")

print("Unión de polígonos…")
# Usamos union_all() como sugiere el warning
municipio_poly = gdf.geometry.unary_union

# 2. Función auxiliar para invertir coordenadas recursivamente
#    (Necesario porque GeoJSON es Lon, Lat, pero H3 espera Lat, Lon)
def swap_coords(coords):
    # Caso base: es una tupla de coordenadas (lon, lat)
    if isinstance(coords[0], (int, float)):
        return (coords[1], coords[0]) # Devuelve (Lat, Lon)
    # Caso recursivo: es una lista de anillos (MultiPolygon)
    return [swap_coords(c) for c in coords]

# 3. Pre-procesar la geometría del municipio
#    Invertimos las coordenadas para el formato (Lat, Lon) que espera h3.polyfill
geo_interface = municipio_poly.__geo_interface__
input_geometry = {
    'type': geo_interface['type'],
    'coordinates': swap_coords(geo_interface['coordinates'])
}

# -----------------------------
# 4. CONEXIÓN DB (¡Faltaba esto!)
# -----------------------------
print("\nConectando a PostgreSQL...")
try:
    engine = create_engine(PG_CONN)
    print("Conexión exitosa.")
except Exception as e:
    print(f"Error al conectar a DB: {e}")
    exit()

# -----------------------------
# 5. GENERACIÓN E INSERCIÓN POR RESOLUCIÓN
# -----------------------------
for RES in RES_LIST:

    print(f"\n=== Generando hexágonos RES={RES} ===")

    # polyfill utiliza la geometría pre-procesada
    hexes = h3.polyfill(input_geometry, RES)

    print(f"Total hexágonos generados: {len(hexes)}")

    rows = []
    for h in hexes:

        # Obtiene el contorno (lat, lon)
        boundary_latlon = h3.h3_to_geo_boundary(h)

        # Convertir a (lon, lat) para Shapely (geom)
        boundary_corrected = [(lon, lat) for lat, lon in boundary_latlon]
        polygon = Polygon(boundary_corrected)

        # centroide (lat, lon -> lon, lat para Point)
        lat_c, lon_c = h3.h3_to_geo(h)
        centroid = Point(lon_c, lat_c)

        rows.append({
            "h3index": h,
            "res": RES,
            "geom": polygon,
            "centroid": centroid
        })

    # GeoDataFrame final
    gdf_hex = gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")

    # Si es la primera vez (RES=9), reemplaza la tabla. Si es RES=10, añade.
    if RES == RES_LIST[0]:
        if_exists_mode = "replace"
    else:
        if_exists_mode = "append"

    # Insertar en PostGIS
    gdf_hex.to_postgis(
        "dim_h3",
        engine,
        if_exists=if_exists_mode,
        index=False
    )

    print(f"✓ RES {RES} insertado correctamente en dim_h3 (Modo: {if_exists_mode}).")

print("\nFIN — Proceso completado.")
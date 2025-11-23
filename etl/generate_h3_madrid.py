import geopandas as gpd
import h3
from shapely.geometry import Polygon, Point
from sqlalchemy import create_engine

# ------------------------------------------------------
# CONFIG
# ------------------------------------------------------
PATH_SHP = "data/raw/BARRIOS.shp"   # <-- Cambia si tu path es distinto
RES = 8
DB_CONN = "postgresql://postgres:postgres@localhost:5432/spatia"

# ------------------------------------------------------
# CARGAR SHAPEFILE + REPROYECTAR A WGS84 (lat/lon)
# ------------------------------------------------------
print("Cargando shapefile de barrios de Madrid...")
gdf = gpd.read_file(PATH_SHP)

print(f"CRS original: {gdf.crs}")
gdf = gdf.to_crs(epsg=4326)
print(f"CRS reproyectado a EPSG:4326 (OK): {gdf.crs}")

print(f"Barrios cargados: {len(gdf)}")

# ------------------------------------------------------
# UNIR TODAS LAS GEOMETRÍAS EN UN POLÍGONO ÚNICO
# ------------------------------------------------------
print("Uniendo barrios en un único polígono del municipio...")

# dissolve genera un único MultiPolygon válido
municipio_poly = gdf.dissolve().geometry.iloc[0]

print("¿Polígono válido?:", municipio_poly.is_valid)
print("Bounds del polígono:", municipio_poly.bounds)

# Convertimos a GeoJSON interface
poly_geojson = municipio_poly.__geo_interface__

# ------------------------------------------------------
# GENERAR HEXÁGONOS H3 (polyfill)
# ------------------------------------------------------
print(f"Generando hexágonos H3 (res={RES})...")
hexes = h3.polyfill(poly_geojson, RES)

print(f"Total hexágonos generados: {len(hexes)}")

if len(hexes) == 0:
    print("\n⚠️ ERROR: polyfill devolvió 0 hexágonos.")
    print("Revisa que el shapefile corresponda al municipio completo y esté en EPSG:4326.\n")
    exit()

# ------------------------------------------------------
# CONVERTIR HEXÁGONOS A GEOMETRÍAS SHAPELY
# ------------------------------------------------------
rows = []

print("Convirtiendo hexágonos a geometrías...")
for h in hexes:
    boundary = h3.h3_to_geo_boundary(h, geo_json=False)  # devuelve [(lat, lon)]
    centroid = h3.h3_to_geo(h)

    poly_geom = Polygon([(lng, lat) for lat, lng in boundary])
    centroid_geom = Point(centroid[1], centroid[0])

    rows.append({
        "h3index": h,
        "resolution": RES,
        "geometry": poly_geom,
        "centroid": centroid_geom
    })

gdf_hex = gpd.GeoDataFrame(rows, crs="EPSG:4326", geometry="geometry")

print("Primeras filas del GeoDataFrame:")
print(gdf_hex.head())

# ------------------------------------------------------
# INSERTAR EN POSTGIS
# ------------------------------------------------------
print("\nInsertando hexágonos en PostGIS tabla dim_h3...")

engine = create_engine(DB_CONN)

gdf_hex.to_postgis(
    "dim_h3",
    con=engine,
    if_exists="append",
    index=False
)

print("\n🎉 ¡ÉXITO! Hexágonos insertados en la tabla dim_h3.")

import geopandas as gpd
from shapely.geometry import Polygon, Point
from sqlalchemy import create_engine

# librería h3 robusta para Windows
from h3ronpy import vector as h3v

# ------------------------------------------------------
# CONFIG
# ------------------------------------------------------
PATH_SHP = "data/raw/BARRIOS.shp"
RES = 8
DB_CONN = "postgresql://postgres:postgres@localhost:5432/spatia"

# ------------------------------------------------------
# CARGAR SHAPEFILE DE BARRIOS
# ------------------------------------------------------
print("Cargando shapefile de barrios de Madrid...")
gdf = gpd.read_file(PATH_SHP)

print(f"Barrios cargados: {len(gdf)}")

# ------------------------------------------------------
# UNIR GEOMETRÍAS
# ------------------------------------------------------
print("Uniendo barrios en un único polígono...")
municipio_poly = gdf.geometry.unary_union

# ------------------------------------------------------
# GENERAR H3 CON H3RONPY
# ------------------------------------------------------
print(f"Generando hexágonos H3 (res={RES})...")

hexes = h3v.polygon_to_cells(
    municipio_poly,
    resolution=RES
)

print(f"Total hexágonos generados: {len(hexes)}")

# ------------------------------------------------------
# CREAR GEOMETRÍAS PARA POSTGIS
# ------------------------------------------------------
rows = []

for hex_id in hexes:
    # boundary en lon/lat
    boundary = h3v.cell_to_boundary(hex_id)
    poly_geom = Polygon(boundary)

    # centroid
    lat, lon = h3v.cell_to_latlng(hex_id)
    centroid_geom = Point(lon, lat)

    rows.append({
        "h3index": hex_id,
        "resolution": RES,
        "geom": poly_geom,
        "centroid": centroid_geom
    })

gdf_hex = gpd.GeoDataFrame(rows, crs="EPSG:4326")

# ------------------------------------------------------
# INSERTAR EN POSTGIS
# ------------------------------------------------------
print("Insertando en PostGIS...")

engine = create_engine(DB_CONN)

gdf_hex.to_postgis(
    "dim_h3",
    con=engine,
    if_exists="append",
    index=False
)

print("¡ÉXITO TOTAL! Hexágonos escritos en dim_h3")

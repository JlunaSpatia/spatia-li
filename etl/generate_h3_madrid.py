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
RES = 8

print("Cargando shapefile…")
gdf = gpd.read_file(PATH_SHP)

if gdf.crs != "EPSG:4326":
    print("Reproyectando a EPSG:4326…")
    gdf = gdf.to_crs("EPSG:4326")

print("Unión de polígonos…")
municipio_poly = gdf.geometry.unary_union

# --- CORRECCIÓN AQUÍ ---
# 1. Obtenemos el GeoJSON (que viene en Lon, Lat)
geo_interface = municipio_poly.__geo_interface__

# 2. Función auxiliar para invertir coordenadas recursivamente
#    (necesario porque puede ser Polygon o MultiPolygon)
def swap_coords(coords):
    # Si es una tupla de coordenadas (lon, lat), inviértela
    if isinstance(coords[0], (int, float)):
        return (coords[1], coords[0]) # Devuelve (Lat, Lon)
    # Si es una lista de listas (anillos), iterar
    return [swap_coords(c) for c in coords]

# 3. Invertimos las coordenadas de la geometría de entrada
#    H3 necesita la entrada como: {'type': ..., 'coordinates': [((Lat, Lon), ...)]}
input_geometry = {
    'type': geo_interface['type'],
    'coordinates': swap_coords(geo_interface['coordinates'])
}

print("Generando hexágonos…")
# Ahora h3 recibe (Lat, Lon) y generará los índices correctos en Madrid
hexes = h3.polyfill(input_geometry, RES)
print(f"Total hexágonos: {len(hexes)}")

rows = []
for h in hexes:
    # boundary devuelve [(lat, lon), ...]
    boundary_latlon = h3.h3_to_geo_boundary(h, geo_json=False)
    
    # Aquí invertimos de nuevo para crear el Polígono de Shapely (Lon, Lat)
    # Esto ya lo tenías bien, pero ahora 'h' corresponderá a Madrid.
    boundary_lonlat = [(lon, lat) for lat, lon in boundary_latlon]
    
    polygon = Polygon(boundary_lonlat)
    
    # centroid: h3.h3_to_geo devuelve (lat, lon)
    lat, lon = h3.h3_to_geo(h)
    centroid = Point(lon, lat) # Shapely quiere (lon, lat)

    rows.append({
        "h3index": h,
        "geom": polygon,
        "centroid": centroid
    })

gdf_hex = gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")

# ... (Resto del código de inserción a SQL igual) ...

engine = create_engine(PG_CONN)
gdf_hex.to_postgis("dim_h3", engine, if_exists="replace", index=False)
print("FIN — Hexágonos correctamente insertados.")
import requests
import numpy as np
from shapely.geometry import Polygon
import geopandas as gpd
from sqlalchemy import create_engine

class IsochroneService:
    def __init__(self, db_url, osrm_url="http://localhost:5001"):
        self.engine = create_engine(db_url)
        self.osrm_url = osrm_url

    def calculate_and_save(self, points_list, minutes=15, table_name="catchment_areas", schema="core"):
        """
        points_list: [{'id': 'h3_index', 'lat': 0.0, 'lon': 0.0}, ...]
        """
        results = []
        seconds = minutes * 60
        
        for p in points_list:
            angles = np.linspace(0, 2 * np.pi, 24)
            radius = 0.015 
            
            destinations = []
            for a in angles:
                d_lat = p['lat'] + (radius * np.cos(a))
                d_lon = p['lon'] + (radius * np.sin(a))
                destinations.append(f"{d_lon},{d_lat}")

            dest_str = ";".join(destinations)
            url = f"{self.osrm_url}/table/v1/foot/{p['lon']},{p['lat']};{dest_str}?sources=0"
            
            try:
                r = requests.get(url, timeout=5).json()
                durations = r['durations'][0][1:]
                
                poly_points = []
                for i, d in enumerate(durations):
                    t_lon, t_lat = map(float, destinations[i].split(','))
                    ratio = min(1, seconds / d) if d and d > 0 else 0.1
                    poly_points.append((
                        p['lon'] + (t_lon - p['lon']) * ratio, 
                        p['lat'] + (t_lat - p['lat']) * ratio
                    ))
                
                results.append({
                    'h3_id': p['id'], # Cambiado de origin_id a h3_id
                    'geometry': Polygon(poly_points),
                    'city': 'MADRID' # Añadimos la ciudad para facilitar filtros posteriores
                })
            
            except Exception as e:
                print(f"⚠️ Error en punto {p['id']}: {e}")
                continue

        if results:
            gdf = gpd.GeoDataFrame(results, crs="EPSG:4326")
            # Usamos el parámetro schema dinámico
            gdf.to_postgis(table_name, self.engine, schema=schema, if_exists='append', index=False)
            return len(results)
        return 0
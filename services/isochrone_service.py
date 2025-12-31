import requests
import numpy as np
from shapely.geometry import Polygon
import geopandas as gpd
from sqlalchemy import create_engine

class IsochroneService:
    def __init__(self, db_url, osrm_url="http://localhost:5001"):
        self.engine = create_engine(db_url)
        self.osrm_url = osrm_url

    def calculate_and_save(self, points_list, minutes, table_name=None, schema="analytics", id_column="origin_id"):
        """
        points_list: [{'id': 'X', 'lat': 0.0, 'lon': 0.0}]
        id_column: 'origin_id' para clientes, 'h3_id' para ciudades.
        """
        # Generar nombre de tabla si no se da uno (ej: catchment_10m)
        target_table = table_name if table_name else f"catchment_{minutes}m"
        
        results = []
        seconds = minutes * 60
        
        for p in points_list:
            # Radio adaptativo según los minutos (aprox 100m por minuto)
            radius = 0.0009 * minutes 
            angles = np.linspace(0, 2 * np.pi, 24)
            destinations = [f"{p['lon'] + (radius * np.sin(a))},{p['lat'] + (radius * np.cos(a))}" for a in angles]
            
            url = f"{self.osrm_url}/table/v1/foot/{p['lon']},{p['lat']};{';'.join(destinations)}?sources=0"
            
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
                    id_column: p['id'],
                    'minutes': minutes,
                    'geometry': Polygon(poly_points)
                })
            except Exception as e:
                print(f"⚠️ Error en punto {p['id']}: {e}")

        if results:
            gdf = gpd.GeoDataFrame(results, crs="EPSG:4326")
            gdf.to_postgis(target_table, self.engine, schema=schema, if_exists='append', index=False)
            return len(results)
        return 0
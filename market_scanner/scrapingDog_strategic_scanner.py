import osmnx as ox
import pandas as pd
import time
import os
import sys
import warnings

warnings.filterwarnings("ignore")

# ================= CONFIGURACIÓN DE OBJETIVOS =================
# Pon aquí las ciudades que quieres "mapear" gratis antes de escanear.
# Formato: "Nombre, Pais"
TARGET_CITIES = [
    "Alcalá de Henares, Spain"
]

OSM_TAGS = {
    'amenity': ['restaurant', 'bar', 'cafe', 'pub', 'ice_cream', 'fast_food', 'pharmacy'],
    'shop': ['clothes', 'shoes', 'bakery', 'supermarket', 'sports', 'boutique', 'books'],
    'leisure': ['fitness_centre', 'sports_centre', 'dance', 'gym'],
    'sport': ['fitness', 'yoga', 'pilates', 'crossfit']
}

def scan_cities():
    print(f"🚀 INICIANDO RADAR OSM UNIVERSAL")
    print(f"🎯 Ciudades a mapear: {len(TARGET_CITIES)}")
    print("-" * 60)

    base_dir = os.path.join("data", "osm_radar")
    os.makedirs(base_dir, exist_ok=True)

    for place_query in TARGET_CITIES:
        # Limpieza de nombre para el archivo (ej: "Alcalá de Henares, Spain" -> "ALCALA_DE_HENARES")
        safe_name = place_query.split(",")[0].upper().replace(" ", "_").replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N")
        
        print(f"\n📍 Procesando: {place_query}...")
        
        try:
            # 1. Descarga
            print(f"   🌍 Descargando datos OSM...")
            gdf = ox.features_from_place(place_query, OSM_TAGS)
            
            if gdf.empty:
                print(f"   ⚠️ No hay datos. Saltando.")
                continue

            # 2. Geometría (Centroides)
            # Proyección a metros -> Centroide -> Proyección a Grados (Lat/Lon)
            gdf['geometry'] = gdf.to_crs(epsg=3857).centroid.to_crs(epsg=4326)
            gdf['latitude'] = gdf.geometry.y
            gdf['longitude'] = gdf.geometry.x

            # 3. Selección y Limpieza
            cols_wanted = ['name', 'latitude', 'longitude', 'amenity', 'shop', 'leisure', 'sport']
            cols_present = [c for c in cols_wanted if c in gdf.columns]
            
            df = pd.DataFrame(gdf[cols_present])
            df = df.dropna(subset=['name']) 
            
            count = len(df)
            print(f"   ✅ Detectados {count} locales potenciales.")

            # 4. Guardado
            # Guardamos con el prefijo OSM_RADAR_ para que el script V5 lo encuentre automático
            filename = os.path.join(base_dir, f"OSM_RADAR_{safe_name}.csv")
            df.to_csv(filename, index=False)
            print(f"   💾 Guardado en: {filename}")
            
            time.sleep(1)

        except Exception as e:
            print(f"   ❌ Error: {e}")
            continue

    print("-" * 60)
    print(f"🏁 PROCESO TERMINADO. Ya puedes lanzar el Escáner V5.")

if __name__ == "__main__":
    scan_cities()
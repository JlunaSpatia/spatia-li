import requests
import json
import time
import os
import sys
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from sqlalchemy import create_engine, text

# ================= SETUP DE ENTORNO =================
script_path = os.path.abspath(__file__)
market_scanner_dir = os.path.dirname(script_path)
project_root = os.path.dirname(market_scanner_dir)

if market_scanner_dir not in sys.path: sys.path.insert(0, market_scanner_dir)
if project_root not in sys.path: sys.path.append(project_root)

try:
    # Importamos SOLO lo necesario de tu conf.py
    import conf as config
    from conf import DB_CONNECTION_STR, SCRAPINGDOG_API_KEY, ACTIVE_CITIES
    print(f"✅ Configuración cargada. Ciudades activas: {ACTIVE_CITIES}")
except ImportError:
    print("❌ Error crítico: No encuentro 'conf.py'. Asegúrate de que está en la carpeta."); sys.exit(1)

# ================= 1. LISTA "MEGA-FAT" (COBERTURA TOTAL) =================
def get_smart_search_terms():
    """
    Lista definitiva combinando categorías generales y nichos específicos.
    """
    return [
        # --- FITNESS & WELLNESS ---
        "Gimnasio", "Centro de Pilates", "Centro de Yoga", "Crossfit", 
        "Entrenador personal", "Estudio de entrenamiento", "Escuela de baile", 
        "Artes marciales", "Club deportivo", "Boxeo", "Fisioterapia",
        "Centro de bienestar", "Detox", 

        # --- RETAIL MODA ---
        "Tienda de ropa",           # Genérico
        "Tienda de ropa de mujer",  # Clave (Brandy, eseOese)
        "Tienda de ropa de hombre", 
        "Tienda de ropa juvenil",   # Clave (Brownie, Renatta)
        "Boutique de moda",         # Clave barrio rico
        "Zapatería", 
        "Sneakers",                 # <--- NICHO (Jefa Sneakers)
        "Tienda de zapatillas",
        "Tienda de ropa vintage",   # <--- NICHO (Justicia/Malasaña)
        "Tienda de accesorios", 
        "Joyería",
        "Tienda de deportes",       
        "Ropa deportiva",

        # --- HOSTELERÍA & OCIO ---
        "Restaurante",              
        "Restaurante de moda", 
        "Bar",                      
        "Bar de tapas",             # Clave Madrid
        "Bar de copas",             # Clave Noche
        "Bar gay",                  # <--- NICHO (LL Bar)
        "Pub",
        "Cervecería",
        "Vinoteca",                 
        "Coctelería",               # <--- NICHO (Mojito's)
        "Cafetería", 
        "Cafetería de especialidad", # Clave Hipster
        "Brunch",                   
        "Heladería",                # <--- NICHO (Mistura)
        "Comida rápida", 
        "Hamburguesería",
        "Pizzería",
        "Comida saludable",         
        "Restaurante vegano",

        # --- ALIMENTACIÓN & SERVICIOS ---
        "Supermercado", 
        "Tienda de alimentación",
        "Mercado gastronómico",     
        "Panadería", 
        "Pastelería",               
        "Carnicería",               
        "Frutería",
        "Tienda gourmet",           
        "Farmacia", 
        "Centro comercial",
        "Tienda de cosméticos",
        "Estanco"
    ]

# ================= 2. CARGA DEL RADAR OSM COMPLETO =================
def load_full_osm_radar():
    """
    Carga todos los CSVs generados en la fase 1 (OSM) para crear un mapa de calor.
    """
    path = os.path.join(project_root, "data", "osm_radar", "OSM_RADAR_*.csv")
    files = glob.glob(path)
    
    if not files:
        print("⚠️ ALERTA: No hay datos de Radar OSM en 'data/osm_radar/'.")
        print("   El script funcionará, pero usará densidad 'MEDIA' por defecto (menos eficiente).")
        return pd.DataFrame(columns=['latitude', 'longitude'])
    
    print(f"📡 Cargando {len(files)} archivos de Radar OSM para análisis de densidad...")
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Aseguramos que tenga columnas lat/lon
            if 'latitude' in df.columns and 'longitude' in df.columns:
                df_list.append(df[['latitude', 'longitude']])
        except: pass
        
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        print(f"   ✅ Mapa de calor generado con {len(full_df)} puntos de referencia.")
        return full_df
    else:
        return pd.DataFrame(columns=['latitude', 'longitude'])

# ================= 3. CEREBRO ADAPTATIVO (OSM -> GRID) =================
def generate_adaptive_targets(hex_geometry, osm_df):
    """
    Analiza la densidad OSM dentro del hexágono y decide la estrategia:
    - BAJA: 1 Disparo (Ahorro)
    - MEDIA: Malla 400m
    - ALTA: Malla 180m (Francotirador)
    """
    targets = []
    minx, miny, maxx, maxy = hex_geometry.bounds
    
    # 1. Contamos cuántos puntos OSM caen en el Bounding Box del hexágono
    # (Usamos filtro vectorial de Pandas por velocidad)
    points_in_box = osm_df[
        (osm_df['longitude'] >= minx) & (osm_df['longitude'] <= maxx) &
        (osm_df['latitude'] >= miny) & (osm_df['latitude'] <= maxy)
    ]
    
    density_count = len(points_in_box)
    
    # --- ESTRATEGIA ADAPTATIVA ---
    
    # CASO A: ZONA "FRÍA" (Monte, Industrial, Residencial disperso)
    if density_count <= 5:
        p = hex_geometry.centroid
        targets.append({
            "coords": f"@{p.y:.6f},{p.x:.6f},15z",
            "type": "LOW_DENSITY (1 Shot)"
        })
        
    # CASO B: ZONA "TEMPLADA" (Barrio estándar)
    elif density_count <= 50:
        step = 0.004 # ~400 metros
        x_rng = np.arange(minx, maxx, step)
        y_rng = np.arange(miny, maxy, step)
        for x in x_rng:
            for y in y_rng:
                p = Point(x + step/2, y + step/2)
                if hex_geometry.contains(p):
                    targets.append({
                        "coords": f"@{p.y:.6f},{p.x:.6f},16z",
                        "type": "MID_DENSITY (400m)"
                    })

    # CASO C: ZONA "HOT" (Justicia, Sol, Salamanca) - MODO FRANCOTIRADOR
    else:
        # Aquí cerramos la malla para pillar Jefa Sneakers, Mistura, etc.
        step = 0.0018 # ~180 metros
        x_rng = np.arange(minx, maxx, step)
        y_rng = np.arange(miny, maxy, step)
        for x in x_rng:
            for y in y_rng:
                p = Point(x + step/2, y + step/2)
                if hex_geometry.contains(p):
                    targets.append({
                        "coords": f"@{p.y:.6f},{p.x:.6f},17z", # Zoom Alto
                        "type": "HIGH_DENSITY (180m)"
                    })
    
    return targets

# ================= 4. MOTOR PRINCIPAL =================
def run_city_scanner():
    engine = create_engine(DB_CONNECTION_STR)
    osm_radar = load_full_osm_radar()
    search_terms = get_smart_search_terms()
    
    base_dir = os.path.join(project_root, "data", "raw", "2025_Q4")
    os.makedirs(base_dir, exist_ok=True)

    for city in ACTIVE_CITIES:
        print(f"\n🚜 INICIANDO ESCANEO INTELIGENTE PARA: {city}")
        
        # 1. Obtenemos los Hexágonos de la DB
        query = text(f"SELECT geometry FROM retail_hexagons WHERE UPPER(city) = UPPER('{city}')")
        try:
            print("🛢️  Consultando hexágonos en DB (PostGIS)...")
            gdf_hex = gpd.read_postgis(query, engine, geom_col='geometry')
            
            # Aseguramos proyección WGS84 (Lat/Lon)
            if gdf_hex.crs and gdf_hex.crs.to_string() != "EPSG:4326":
                gdf_hex = gdf_hex.to_crs(epsg=4326)
                
            print(f"   ✅ Hexágonos recuperados: {len(gdf_hex)}")
        except Exception as e:
            print(f"❌ Error DB: {e}"); continue
            
        if gdf_hex.empty: 
            print("⚠️ No hay hexágonos para esta ciudad. Saltando."); continue

        # 2. Generamos los objetivos (Targets) hexágono a hexágono
        all_targets = []
        print("🧠 Calculando densidad y generando malla adaptativa...")
        
        stats = {"LOW": 0, "MID": 0, "HIGH": 0}
        
        for idx, row in gdf_hex.iterrows():
            targets = generate_adaptive_targets(row.geometry, osm_radar)
            for t in targets:
                t['hex_id'] = idx
                all_targets.append(t)
                # Estadística simple
                if "LOW" in t['type']: stats["LOW"] += 1
                elif "MID" in t['type']: stats["MID"] += 1
                else: stats["HIGH"] += 1
        
        print(f"🎯 PLAN DE ATAQUE GENERADO:")
        print(f"   ❄️  Low Density (Ahorro): {stats['LOW']} puntos")
        print(f"   🌤️  Mid Density (Normal): {stats['MID']} puntos")
        print(f"   🔥  High Density (Sniper): {stats['HIGH']} puntos (Aquí cazamos nichos)")
        print(f"   -----------------------------------")
        print(f"   TOTAL Peticiones Geográficas: {len(all_targets)}")
        
        # 3. Escaneo
        filename = os.path.join(base_dir, f"{city}_ADAPTIVE_V5.json")
        checkpoint_file = os.path.join(base_dir, f"{city}_checkpoint_V5.json")
        
        master_db = {}
        completed_coords = [] 
        
        # Cargar estado previo
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as f:
                    d = json.load(f)
                    for i in d: master_db[i['place_id']] = i
            except: pass
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    completed_coords = json.load(f).get("completed", [])
            except: pass

        print(f"📚 Datos previos cargados: {len(master_db)} locales. Puntos completados: {len(completed_coords)}")

        # Bucle de Disparo
        try:
            for t in all_targets:
                coord_id = t['coords'] # Usamos la string @lat,lon,zoom como ID único
                
                if coord_id in completed_coords: continue
                
                print(f"\n📍 Escaneando [{t['type']}]: {coord_id}")
                
                # Bucle de Términos
                for term in search_terms:
                    print(f"   🔍 {term}...", end="", flush=True)
                    
                    page = 0
                    new_count = 0
                    # Bajamos hasta 2 páginas (suficiente con malla fina)
                    while page < 2: 
                        params = {
                            "api_key": SCRAPINGDOG_API_KEY, 
                            "query": term, 
                            "ll": coord_id, 
                            "page": page * 20, "language": "es"
                        }
                        try:
                            r = requests.get("https://api.scrapingdog.com/google_maps", params=params, timeout=30)
                            if r.status_code == 200:
                                res = r.json().get("search_results", [])
                                if not res: break
                                
                                for item in res:
                                    pid = item.get("place_id")
                                    if pid:
                                        # Enriquecemos el dato
                                        item['_scraped_term'] = term
                                        item['_city'] = city
                                        item['_density_type'] = t['type']
                                        
                                        if pid not in master_db:
                                            master_db[pid] = item
                                            new_count += 1
                                            
                                if len(res) < 20: break # Si hay menos de 20, no hay página siguiente
                                page += 1
                                time.sleep(0.5) # Pausa de cortesía
                            else: 
                                print(f"(Err:{r.status_code})", end="")
                                break
                        except Exception as e: 
                            print(f"(Ex:{e})", end="")
                            break
                    
                    if new_count > 0: print(f" +{new_count}", end="")
                    else: print(".", end="")

                completed_coords.append(coord_id)
                
                # Guardado de seguridad cada punto
                with open(filename, 'w') as f: json.dump(list(master_db.values()), f, indent=4, ensure_ascii=False)
                with open(checkpoint_file, 'w') as f: json.dump({"completed": completed_coords}, f)

        except KeyboardInterrupt:
            print("\n🛑 Pausado por usuario.")
        
        print(f"\n✅ Ciudad {city} finalizada. Total locales únicos: {len(master_db)}")

if __name__ == "__main__":
    run_city_scanner()
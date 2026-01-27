import requests
import json
import time
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from sqlalchemy import create_engine, text

# ================= SETUP =================
script_path = os.path.abspath(__file__)
market_scanner_dir = os.path.dirname(script_path)
project_root = os.path.dirname(market_scanner_dir)

if market_scanner_dir not in sys.path:
    sys.path.insert(0, market_scanner_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    import conf as config
    from conf import DB_CONNECTION_STR, SCRAPINGDOG_API_KEY, ZOOM_LEVEL, GRID_STEP
    print("✅ Configuración cargada correctamente.")
except ImportError:
    print(f"❌ Error crítico: No encuentro 'conf.py'.")
    sys.exit(1)

# ================= 1. LISTA DE BÚSQUEDA "FAT LIST" (ACTIVA) =================
def get_smart_search_terms():
    """
    Lista completa para recuperar nichos (Brandy, Daily Bar) y tráfico general.
    """
    print("🧠 Cargando lista EXTENDIDA (Máxima Cobertura)...")
    return [
        # --- BLOQUE 1: FITNESS & WELLNESS ---
        "Gimnasio", "Centro de Pilates", "Centro de Yoga", "Crossfit", 
        "Entrenador personal", "Estudio de entrenamiento", "Escuela de baile", 
        "Artes marciales", "Club deportivo", "Boxeo", "Fisioterapia",
        
        # --- BLOQUE 2: RETAIL MODA ---
        "Tienda de ropa",           # Genérico
        "Tienda de ropa de mujer",  # <--- CLAVE (Brandy, eseOese)
        "Tienda de ropa de hombre", 
        "Tienda de ropa juvenil",   # <--- CLAVE (Brownie, Renatta)
        "Boutique de moda",         # <--- CLAVE (Tiendas de barrio rico)
        "Zapatería", 
        "Tienda de accesorios", 
        "Joyería",
        "Tienda de deportes",       
        "Ropa deportiva",

        # --- BLOQUE 3: HOSTELERÍA & OCIO ---
        "Restaurante",              # Genérico
        "Restaurante de moda", 
        "Bar",                      # Genérico
        "Bar de tapas",             # <--- CLAVE MADRID
        "Bar de copas",             # <--- CLAVE NOCHE (Daily Bar)
        "Cervecería",
        "Vinoteca",                 
        "Cafetería", 
        "Cafetería de especialidad", # <--- CLAVE HIPSTER
        "Brunch",                   
        "Comida rápida", 
        "Hamburguesería",
        "Pizzería",
        "Comida saludable",         
        "Restaurante vegano",

        # --- BLOQUE 4: ALIMENTACIÓN & SERVICIOS ---
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
        "Tienda de cosméticos"      
    ]

# ================= 2. GENERADOR DE MALLA (GRID) =================
def generate_grid_points(polygon, step_degrees=0.004):
    minx, miny, maxx, maxy = polygon.bounds
    x_coords = np.arange(minx, maxx, step_degrees)
    y_coords = np.arange(miny, maxy, step_degrees)
    
    grid_points = []
    for x in x_coords:
        for y in y_coords:
            p = Point(x, y)
            if polygon.contains(p):
                grid_points.append(p)
    
    if not grid_points: return [polygon.centroid]
    return grid_points

# ================= 3. OBTENCIÓN DE TARGETS (SOLO JUSTICIA) =================
def get_study_targets(study_name):
    engine = create_engine(DB_CONNECTION_STR)
    
    # Mantenemos el filtro de JUSTICIA y el arreglo de coordenadas (4326)
    query = text(f"""
        SELECT location_name, ST_Transform(geometry, 4326) as geometry 
        FROM analytics.study_catchments 
        WHERE study_name = '{study_name}' 
          AND location_name = 'Justicia_AugustoFigueroa_Hipster'
    """)
    
    print(f"🛢️ Consultando PostGIS para: {study_name} (SOLO JUSTICIA)...")
    
    try:
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        if gdf.empty: 
            print("⚠️ No se encontró la zona 'Justicia_AugustoFigueroa_Hipster'.")
            return []
        
        search_targets = []
        step = getattr(config, 'GRID_STEP', 0.004)

        for idx, row in gdf.iterrows():
            poly = row.geometry
            zone_name = row['location_name']
            points = generate_grid_points(poly, step_degrees=step)
            
            print(f"   📐 Zona '{zone_name}': {len(points)} puntos de malla.")
            
            for i, pt in enumerate(points):
                target_id = f"{zone_name}_GRID_{i+1}"
                coords_str = f"@{pt.y:.6f},{pt.x:.6f},{ZOOM_LEVEL}"
                search_targets.append({
                    "zone_name": zone_name,
                    "point_id": target_id,
                    "coords": coords_str
                })
        return search_targets
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return []

# ================= 4. MOTOR DE ESCANEO =================
def run_study_scanner(study_name):
    base_dir = os.path.join(project_root, "data", "raw", "studies")
    os.makedirs(base_dir, exist_ok=True)
    
    filename = os.path.join(base_dir, f"{study_name}_RAW_V2.json")
    checkpoint_file = os.path.join(base_dir, f"{study_name}_checkpoint_V2.json")

    targets = get_study_targets(study_name)
    search_terms = get_smart_search_terms()

    if not targets: return

    # Profundidad: 2 páginas por término por punto de malla (suficiente con el grid)
    PAGES_PER_POINT = 2 
    total_requests = len(targets) * len(search_terms) * PAGES_PER_POINT
    
    print("-" * 60)
    print(f"💰 MODO PRODUCCIÓN V2 (Lista Completa)")
    print(f"   Archivo Salida: {os.path.basename(filename)}")
    print(f"   Puntos (Grid): {len(targets)}")
    print(f"   Términos: {len(search_terms)}")
    print(f"   Peticiones Est.: ~{total_requests}")
    print("-" * 60)

    # Cargar Estado
    master_db = {} 
    completed_points = []
    
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                for i in data: master_db[i['place_id']] = i
            print(f"📚 Datos previos V2 recuperados: {len(master_db)}")
        except: pass
        
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f: 
                completed_points = json.load(f).get("completed_points", [])
        except: pass

    try:
        for t in targets:
            point_id = t['point_id']
            zone_real_name = t['zone_name']
            
            if point_id in completed_points:
                print(f"⏩ Saltando completado: {point_id}")
                continue

            print(f"\n📍 Escaneando: {point_id}")
            # print(f"   🌍 Coordenadas: {t['coords']}") # Debug opcional

            for term in search_terms:
                print(f"   🔍 '{term}'...", end="", flush=True)
                page = 0
                new_count = 0
                
                while page < PAGES_PER_POINT: 
                    params = {
                        "api_key": SCRAPINGDOG_API_KEY, 
                        "query": term, 
                        "ll": t['coords'], 
                        "page": page * 20, 
                        "language": "es"
                    }
                    try:
                        r = requests.get("https://api.scrapingdog.com/google_maps", params=params, timeout=30)
                        if r.status_code == 200:
                            res = r.json().get("search_results", [])
                            if not res: break
                            
                            for item in res:
                                pid = item.get("place_id")
                                if pid:
                                    item['_study_name'] = study_name
                                    item['_catchment_zone'] = zone_real_name
                                    item['_scraped_term'] = term
                                    item['_grid_point'] = point_id
                                    if pid not in master_db:
                                        master_db[pid] = item
                                        new_count += 1
                            if len(res) < 20: break
                            page += 1
                            time.sleep(0.5)
                        else: 
                            print(f"(Err:{r.status_code})", end="")
                            break
                    except: break
                print(f" +{new_count}")

            completed_points.append(point_id)
            with open(filename, 'w') as f: json.dump(list(master_db.values()), f, ensure_ascii=False, indent=4)
            with open(checkpoint_file, 'w') as f: json.dump({"completed_points": completed_points}, f)

    except KeyboardInterrupt: print("\n🛑 Pausado.")
    print(f"\n✅ Finalizado V2. Total locales únicos capturados: {len(master_db)}")

if __name__ == "__main__":
    run_study_scanner("GYM_BOUTIQUE_MADRID_001")
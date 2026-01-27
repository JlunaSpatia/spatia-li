import requests
import json
import time
import os
import sys
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
import numpy as np
from sqlalchemy import create_engine, text

# ================= SETUP =================
script_path = os.path.abspath(__file__)
market_scanner_dir = os.path.dirname(script_path)
project_root = os.path.dirname(market_scanner_dir)

if market_scanner_dir not in sys.path: sys.path.insert(0, market_scanner_dir)
if project_root not in sys.path: sys.path.append(project_root)

try:
    import conf as config
    from conf import DB_CONNECTION_STR, SCRAPINGDOG_API_KEY
    print("✅ Configuración cargada para TEST DE VALIDACIÓN.")
except ImportError:
    print("❌ Falta conf.py"); sys.exit(1)

# ================= 1. LISTA MEGA-FAT (COMPLETA) =================
# Esta es la lista que garantiza que no perdemos nichos
def get_smart_search_terms():
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

# ================= 2. CARGA DEL RADAR OSM =================
# ================= 2. CARGA DEL RADAR OSM (CORREGIDO) =================
def load_full_osm_radar():
    # CORRECCIÓN: Usamos market_scanner_dir porque tu carpeta data está dentro de market_scanner
    path = os.path.join(market_scanner_dir, "data", "osm_radar", "OSM_RADAR_*.csv")
    
    print(f"🔍 Buscando radar en: {path}") # Debug para que veas dónde mira
    files = glob.glob(path)
    
    if not files:
        print("⚠️ ALERTA: No encuentro archivos.")
        print("   El script funcionará en 'Modo Ciego'.")
        return pd.DataFrame(columns=['latitude', 'longitude'])

    df_list = []
    print(f"📡 Cargando {len(files)} archivos de Radar OSM...")
    for f in files:
        try:
            df = pd.read_csv(f)
            if 'latitude' in df.columns and 'longitude' in df.columns:
                df_list.append(df[['latitude', 'longitude']])
        except: pass
        
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        print(f"   ✅ Radar cargado con {len(full_df)} puntos de referencia.")
        return full_df
    else:
        return pd.DataFrame(columns=['latitude', 'longitude'])

# ================= 3. CEREBRO ADAPTATIVO (Malla + OSM) =================
def generate_adaptive_targets(zone_geometry, osm_df):
    targets = []
    minx, miny, maxx, maxy = zone_geometry.bounds
    
    # 1. Malla de análisis base (aprox 500m)
    step_analysis = 0.005 
    x_coords = np.arange(minx, maxx, step_analysis)
    y_coords = np.arange(miny, maxy, step_analysis)

    for x in x_coords:
        for y in y_coords:
            # Creamos celda de análisis
            cell_poly = box(x, y, x+step_analysis, y+step_analysis)
            
            # Si no toca la zona de estudio, saltar
            if not zone_geometry.intersects(cell_poly): continue
            
            # --- AQUÍ SE USA OSM ---
            # Contamos cuántos puntos de OSM hay en este cuadrado
            points_in_cell = osm_df[
                (osm_df['longitude'] >= x) & (osm_df['longitude'] < x+step_analysis) &
                (osm_df['latitude'] >= y) & (osm_df['latitude'] < y+step_analysis)
            ]
            count = len(points_in_cell)
            
            # --- DECISIÓN ---
            if count <= 2: # Zona muerta (polígonos vacíos)
                p = cell_poly.centroid
                if zone_geometry.contains(p):
                    targets.append({"coords": f"@{p.y:.6f},{p.x:.6f},15z", "type": "LOW"})
            
            elif count <= 30: # Zona media
                # Dividimos en 4 (250m)
                sub_step = step_analysis / 2
                for sx in np.arange(x, x+step_analysis, sub_step):
                    for sy in np.arange(y, y+step_analysis, sub_step):
                        p = Point(sx+sub_step/2, sy+sub_step/2)
                        if zone_geometry.contains(p):
                            targets.append({"coords": f"@{p.y:.6f},{p.x:.6f},16z", "type": "MID"})
            
            else: # Zona HOT (Oficinas, restaurantes densos)
                # Dividimos en malla fina (150m aprox)
                sub_step = 0.0015 
                for sx in np.arange(x, x+step_analysis, sub_step):
                    for sy in np.arange(y, y+step_analysis, sub_step):
                        p = Point(sx+sub_step/2, sy+sub_step/2)
                        if zone_geometry.contains(p):
                            targets.append({"coords": f"@{p.y:.6f},{p.x:.6f},17z", "type": "HIGH_SNIPER"})
                            
    return targets

# ================= 4. EJECUCIÓN DEL TEST =================
def run_validation_test(study_name):
    engine = create_engine(DB_CONNECTION_STR)
    osm_radar = load_full_osm_radar()
    search_terms = get_smart_search_terms()
    
    print(f"\n🧪 INICIANDO TEST DE VALIDACIÓN: {study_name}")
    
    # 1. Recuperar la zona específica de la DB
    query = text(f"""
        SELECT location_name, ST_Transform(geometry, 4326) as geometry 
        FROM analytics.study_catchments 
        WHERE study_name = '{study_name}'
    """)
    
    try:
        gdf_zones = gpd.read_postgis(query, engine, geom_col='geometry')
        print(f"✅ Zona recuperada: {len(gdf_zones)} polígono(s).")
    except Exception as e:
        print(f"❌ Error DB: {e}"); return

    if gdf_zones.empty:
        print("❌ No se encontró el study_name en la tabla study_catchments."); return

    # 2. Generar Targets
    all_targets = []
    print("🧠 Calculando malla adaptativa (Grid + OSM)...")
    
    for _, row in gdf_zones.iterrows():
        t = generate_adaptive_targets(row.geometry, osm_radar)
        all_targets.extend(t)
        
    print("-" * 60)
    print(f"🎯 PLAN DE ATAQUE ({len(search_terms)} categorías):")
    high_count = len([t for t in all_targets if 'HIGH' in t['type']])
    print(f"   🔥 Puntos Francotirador (180m): {high_count}")
    print(f"   ❄️ Puntos Ahorro: {len(all_targets) - high_count}")
    print(f"   📍 Total Puntos Geográficos: {len(all_targets)}")
    print("-" * 60)
    
    # CONFIRMACIÓN DE SEGURIDAD
    confirm = input("¿Ejecutar gasto de créditos? (SI/NO): ")
    if confirm.upper() != "SI": return

    # 3. Disparo
    base_dir = os.path.join(project_root, "data", "raw", "validation")
    os.makedirs(base_dir, exist_ok=True)
    filename = os.path.join(base_dir, f"{study_name}_RAW.json")
    
    master_db = {}
    
    # Cargar si existe previo
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f: master_db = {i['place_id']: i for i in json.load(f)}
        except: pass

    try:
        for t in all_targets:
            print(f"\n📍 {t['coords']} [{t['type']}]")
            
            for term in search_terms:
                print(f"   🔍 {term}...", end="", flush=True)
                
                # --- API REQUEST ---
                page = 0
                new_items = 0
                while page < 2:
                    params = {
                        "api_key": SCRAPINGDOG_API_KEY, 
                        "query": term, 
                        "ll": t['coords'], 
                        "page": page*20, "language": "es"
                    }
                    try:
                        r = requests.get("https://api.scrapingdog.com/google_maps", params=params, timeout=30)
                        if r.status_code == 200:
                            res = r.json().get("search_results", [])
                            if not res: break
                            for item in res:
                                if item.get("place_id") not in master_db:
                                    item['_src'] = 'VALIDATION'
                                    item['_term'] = term
                                    master_db[item.get("place_id")] = item
                                    new_items += 1
                            if len(res) < 20: break
                            page += 1
                        else: break
                    except: break
                
                if new_items > 0: print(f" +{new_items}", end="")
                else: print(".", end="")
                
            # Guardar en cada paso geográfico
            with open(filename, 'w') as f: json.dump(list(master_db.values()), f, indent=4, ensure_ascii=False)

    except KeyboardInterrupt:
        print("\n🛑 Pausado por usuario.")

    print(f"\n✅ Test Finalizado. Total locales únicos: {len(master_db)}")

if __name__ == "__main__":
    # AQUÍ PONEMOS EL NOMBRE DE TU ZONA DE VALIDACIÓN
    run_validation_test("VALIDACION_MADRID_RETAIL_2")
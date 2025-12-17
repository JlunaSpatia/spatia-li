import requests
import json
import time
import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine

# ================= SETUP ROBUSTO =================
script_path = os.path.abspath(__file__)
market_scanner_dir = os.path.dirname(script_path)
project_root = os.path.dirname(market_scanner_dir)

if market_scanner_dir not in sys.path:
    sys.path.insert(0, market_scanner_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    import config
    from config import DB_CONNECTION_STR, SCRAPINGDOG_API_KEY, GRID_STEP, ZOOM_LEVEL, CATEGORIAS
    print("✅ Configuración cargada.")
except ImportError:
    sys.exit("❌ Error: No encuentro config.py")

# ================= FUNCIONES =================

def get_hybrid_grid(city_name):
    """
    1. Genera puntos matemáticos (cada 1km aprox).
    2. Descarga la capa de hexágonos de la DB.
    3. Cruza ambas capas: Solo devuelve los puntos que caen DENTRO de un hexágono.
    """
    if city_name not in config.CITY_BBOXES:
        print(f"❌ La ciudad {city_name} no está en config.CITY_BBOXES")
        return []

    # 1. Generar Candidatos Matemáticos (Fuerza Bruta)
    print(f"📐 Generando malla matemática (Paso: {GRID_STEP})...")
    bbox = config.CITY_BBOXES[city_name]
    lat_steps = np.arange(bbox["min_lat"], bbox["max_lat"], GRID_STEP)
    lon_steps = np.arange(bbox["min_lon"], bbox["max_lon"], GRID_STEP)
    
    candidate_points = []
    for lat in lat_steps:
        for lon in lon_steps:
            candidate_points.append(Point(lon, lat)) # OJO: Point es (Lon, Lat)
            
    total_candidates = len(candidate_points)
    print(f"   -> {total_candidates} puntos candidatos generados.")

    # 2. Descargar Hexágonos de PostGIS (El Filtro)
    print(f"🛢️  Descargando hexágonos válidos de la DB...")
    engine = create_engine(DB_CONNECTION_STR)
    sql = f"SELECT geometry FROM retail_hexagons WHERE UPPER(city) = UPPER('{city_name}')"
    
    try:
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
        if gdf_hex.empty:
            print("⚠️  ALERTA: No hay hexágonos en la DB para filtrar. Usando malla completa.")
            return [f"@{p.y:.5f},{p.x:.5f},{ZOOM_LEVEL}" for p in candidate_points]
            
        print(f"   -> {len(gdf_hex)} hexágonos cargados como máscara de filtro.")
    except Exception as e:
        print(f"❌ Error conectando a DB: {e}")
        return []

    # 3. El Gran Filtrado (Spatial Join)
    print("⚔️  Cruzando Malla vs Hexágonos...")
    # Creamos GeoDataFrame con los puntos candidatos
    gdf_points = gpd.GeoDataFrame(geometry=candidate_points, crs="EPSG:4326")
    
    # SJOIN: Keep points that are WITHIN hexagons
    gdf_filtered = gpd.sjoin(gdf_points, gdf_hex, how="inner", predicate="within")
    
    final_points = []
    # Extraemos lat/lon de los supervivientes
    for idx, row in gdf_filtered.iterrows():
        lat = row.geometry.y
        lon = row.geometry.x
        final_points.append(f"@{lat:.5f},{lon:.5f},{ZOOM_LEVEL}")

    print(f"✨ FILTRADO COMPLETADO: De {total_candidates} pasamos a {len(final_points)} puntos.")
    print(f"🗑️  Descartados (Mar/Bosque/Vacío): {total_candidates - len(final_points)}")
    
    return final_points

def run_scanner(city_name):
    # Setup carpetas output
    base_dir = os.path.join(project_root, "data", "raw", "2025_Q4")
    os.makedirs(base_dir, exist_ok=True)
    filename = os.path.join(base_dir, f"{city_name}_FULL_RAW.json")
    checkpoint_file = os.path.join(base_dir, f"{city_name}_checkpoint.json")

    print(f"🚜 INICIANDO ESCANEO HÍBRIDO: {city_name}")

    # --- OBTENER MALLA FILTRADA ---
    grid = get_hybrid_grid(city_name)
    total_cells = len(grid)
    
    if total_cells == 0: return

    # --- AUDITORÍA DE COSTE ---
    AVG_PAGES = 1.5 
    COST_PER_REQUEST = 5 
    
    estimated_requests = total_cells * len(CATEGORIAS) * AVG_PAGES
    estimated_credits = estimated_requests * COST_PER_REQUEST
    cost_usd = estimated_credits * 0.0002 

    print("-" * 60)
    print(f"💰 PRESUPUESTO FINAL ({city_name})")
    print(f"   Puntos a Escanear: {total_cells} (Filtrados por DB)")
    print(f"   Categorías: {len(CATEGORIAS)} {CATEGORIAS}")
    print(f"   Coste Estimado: ~${cost_usd:.2f} USD")
    print("-" * 60)
    
    response = input("¿Proceder? (SI/NO): ")
    if response.upper() != "SI": return

    # --- BUCLE DE ESCANEO ---
    completed_cells = []
    all_data = []
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            state = json.load(f)
            completed_cells = state.get("completed_cells", [])
    
    # 🔴 AQUÍ ESTABA EL ERROR: AHORA ESTÁ CORREGIDO (INDENTADO)
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                all_data = json.load(f)
        except:
            pass

    try:
        for i, coords in enumerate(grid):
            if coords in completed_cells: continue
            
            print(f"\n📍 {i+1}/{total_cells}: {coords}")
            cell_data = []
            
            for query in CATEGORIAS:
                print(f"   🔍 {query}...", end="", flush=True)
                page = 0
                while page < 5: 
                    params = {
                        "api_key": SCRAPINGDOG_API_KEY, "query": query, "ll": coords,
                        "page": page * 20, "language": "es"
                    }
                    try:
                        r = requests.get("https://api.scrapingdog.com/google_maps", params=params, timeout=30)
                        if r.status_code == 200:
                            res = r.json().get("search_results", [])
                            if not res: 
                                print(".", end="")
                                break
                            
                            for item in res:
                                if isinstance(item, dict):
                                    item['_scrape_coords'] = coords
                                    item['_category'] = query
                                    item['_city'] = city_name
                            
                            cell_data.extend(res)
                            print(f"{len(res)}", end="")
                            if len(res) < 20: break
                            page += 1
                        else: break
                    except: break
                print(" ok")

            all_data.extend(cell_data)
            completed_cells.append(coords)
            
            with open(filename, 'w') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            with open(checkpoint_file, 'w') as f:
                json.dump({"completed_cells": completed_cells}, f)

    except KeyboardInterrupt:
        print("\n🛑 Pausado.")

if __name__ == "__main__":
    run_scanner("BARCELONA")
import requests
import json
import time
import os
import numpy as np
from datetime import datetime
import config  # Importamos tu configuración

def generate_grid(city_conf):
    lat_steps = np.arange(city_conf["min_lat"], city_conf["max_lat"], config.GRID_STEP)
    lon_steps = np.arange(city_conf["min_lon"], city_conf["max_lon"], config.GRID_STEP)
    grid = []
    for lat in lat_steps:
        for lon in lon_steps:
            grid.append(f"@{lat:.5f},{lon:.5f},{config.ZOOM_LEVEL}")
    return grid

def run_scanner(city_name):
    if city_name not in config.CITIES:
        print(f"❌ Ciudad '{city_name}' no configurada en config.py")
        return

    # Preparar Carpetas
    quarter = "2025_Q1" # Cambiar esto manualmente cada trimestre o hacerlo dinámico
    base_dir = os.path.join("data", "raw", quarter)
    os.makedirs(base_dir, exist_ok=True)
    
    # Archivos
    filename = os.path.join(base_dir, f"{city_name}_FULL_RAW.json")
    checkpoint_file = os.path.join(base_dir, f"{city_name}_checkpoint.json")

    print(f"🚜 INICIANDO ESCANEO: {city_name}")
    print(f"📂 Guardando en: {filename}")

    # Generar Malla
    grid = generate_grid(config.CITIES[city_name])
    print(f"🕸️  Celdas a escanear: {len(grid)}")

    # --- SISTEMA DE RESUME (ANTI-MIEDO) ---
    completed_cells = []
    all_data = []
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            state = json.load(f)
            completed_cells = state.get("completed_cells", [])
            print(f"🔄 REANUDANDO: {len(completed_cells)} celdas ya procesadas anteriormente.")
    
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            all_data = json.load(f)

    # --- BUCLE PRINCIPAL ---
    total_requests_session = 0
    
    try:
        for i, coords in enumerate(grid):
            if coords in completed_cells:
                continue # Saltamos lo que ya pagamos

            print(f"\n📍 Celda {i+1}/{len(grid)}: {coords}")
            
            cell_data = []
            
            for query in config.CATEGORIAS:
                print(f"   🔍 '{query}'...", end="", flush=True)
                page = 0
                while True:
                    # Límite duro de Google (página 6)
                    if page > 6: break 
                    
                    params = {
                        "api_key": config.SCRAPINGDOG_API_KEY,
                        "query": query,
                        "ll": coords,
                        "page": page * 20,
                        "language": "es"
                    }
                    
                    try:
                        r = requests.get("https://api.scrapingdog.com/google_maps", params=params, timeout=30)
                        total_requests_session += 1
                        
                        if r.status_code == 200:
                            data = r.json()
                            results = data.get("search_results", [])
                            
                            if not isinstance(results, list) or not results:
                                print(".", end="")
                                break
                            
                            # Metadata
                            for item in results:
                                if isinstance(item, dict):
                                    item['_scrape_coords'] = coords
                                    item['_category'] = query
                            
                            cell_data.extend(results)
                            print(f"{len(results)}", end="")
                            
                            if len(results) < 20: break
                            page += 1
                            time.sleep(0.5)
                        else:
                            print(f"[Err{r.status_code}]", end="")
                            time.sleep(2)
                            break
                    except Exception as e:
                        print(f"[NetErr]", end="")
                        time.sleep(2)
                        break
                print(" ok.")

            # --- GUARDADO TRANSACCIONAL (CADA CELDA) ---
            all_data.extend(cell_data)
            completed_cells.append(coords)
            
            # Guardamos DATOS
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            
            # Guardamos ESTADO (Checkpoint)
            with open(checkpoint_file, 'w') as f:
                json.dump({"completed_cells": completed_cells}, f)

    except KeyboardInterrupt:
        print("\n🛑 DETENIDO POR USUARIO. El progreso está guardado.")
        print("   Puedes reanudar cuando quieras ejecutando de nuevo.")
        return

    print(f"\n✅ CIUDAD COMPLETADA: {city_name}")
    print(f"📊 Total locales: {len(all_data)}")
    print(f"💸 Peticiones esta sesión: {total_requests_session}")
    
    # Limpieza final: Borrar checkpoint si acabó bien
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

if __name__ == "__main__":
    # CAMBIA ESTO PARA ELEGIR QUÉ DESCARGAR
    run_scanner("LASTABLAS_TEST") 
    # run_scanner("VALENCIA")
    # run_scanner("MADRID")
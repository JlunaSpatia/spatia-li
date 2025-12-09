import requests
import json
import time
import os
import numpy as np
from datetime import datetime
import config  # Importamos tu configuración

# --- GESTIÓN DE LA MALLA (SMART vs MATH) ---
def get_grid_points(city_name, city_conf):
    """
    Intenta cargar la malla inteligente (filtrada por h3 con poblaciñon en nuestra ciudad POSTGIS).
    Si no existe, genera la malla matemática cuadrada completa.
    """
    # 1. Buscar si existe un Smart Grid pre-calculado
    smart_grid_path = os.path.join("market_scanner", "cache", f"{city_name}_SMART_GRID.json")
    
    if os.path.exists(smart_grid_path):
        print(f"🧠 MODO SMART: Cargando malla optimizada desde {smart_grid_path}")
        print(f"   (Se saltarán zonas vacías como El Pardo)")
        with open(smart_grid_path, 'r') as f:
            return json.load(f)
    
    # 2. Si no existe, usar el método matemático (Fuerza bruta)
    print(f"⚠️ MODO MATEMÁTICO: No se encontró Smart Grid en cache.")
    print(f"   Calculando rectángulo completo (incluye zonas vacías)...")
    
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
    quarter = "2025_Q4" # Puedes cambiar esto dinámicamente si quieres
    base_dir = os.path.join("data", "raw", quarter)
    os.makedirs(base_dir, exist_ok=True)
    
    # Archivos
    filename = os.path.join(base_dir, f"{city_name}_FULL_RAW.json")
    checkpoint_file = os.path.join(base_dir, f"{city_name}_checkpoint.json")

    print(f"🚜 INICIANDO ESCANEO: {city_name}")
    print(f"📂 Guardando en: {filename}")

    # --- GENERAR/CARGAR MALLA ---
    grid = get_grid_points(city_name, config.CITIES[city_name])
    total_cells = len(grid)
    print(f"🕸️  Celdas a escanear: {total_cells}")

    # --- INYECCIÓN DE SEGURIDAD: AUDITORÍA DE COSTE ---
    total_categories = len(config.CATEGORIAS)
    AVG_PAGES = 2.5 # Estimación conservadora de 2.5 páginas por búsqueda
    COST_PER_REQUEST = 5 # Créditos por petición
    
    estimated_requests = total_cells * total_categories * AVG_PAGES
    estimated_credits = estimated_requests * COST_PER_REQUEST
    cost_usd = estimated_credits * 0.0002 # ($40 / 200,000 credits)

    print("-" * 60)
    print("💰 AUDITORÍA DE COSTE ANTES DE LANZAMIENTO")
    print(f"   CATEGORÍAS ACTIVAS: {total_categories}")
    print(f"   PUNTOS ÚTILES A ESCANEAR: {total_cells}")
    print(f"   PETICIONES API ESTIMADAS: ~{int(estimated_requests):,}")
    print(f"   CRÉDITOS A GASTAR: ~{int(estimated_credits):,} créditos")
    print(f"   COSTE APROX. (USD): ~${cost_usd:.2f} USD")
    print("-" * 60)
    
    response = input("¿Confirmas el coste y quieres proceder con la descarga? (Escribe 'SI'): ")
    
    if response.upper() != "SI":
        print("🛑 LANZAMIENTO CANCELADO POR EL USUARIO. Archivo intacto.")
        return
    
    # --- CONTINÚA SOLO SI EL USUARIO ESCRIBIÓ 'SI' ---

    # --- SISTEMA DE RESUME (CHECKPOINT) ---
    completed_cells = []
    all_data = []
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            state = json.load(f)
            completed_cells = state.get("completed_cells", [])
            print(f"🔄 REANUDANDO: {len(completed_cells)} celdas ya procesadas anteriormente.")
    
    if os.path.exists(filename):
        # Leemos lo que ya llevamos guardado para no machacarlo
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
        except json.JSONDecodeError:
            all_data = [] # Si el archivo estaba corrupto, empezamos lista vacía

    # --- BUCLE PRINCIPAL (AQUÍ EMPIEZA EL GASTO) ---
    total_requests_session = 0
    
    try:
        for i, coords in enumerate(grid):
            # Si esta coordenada ya está en la lista de completadas, saltar
            if coords in completed_cells:
                continue 

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
                            
                            # Si no hay resultados o la lista está vacía
                            if not isinstance(results, list) or not results:
                                print(".", end="")
                                break
                            
                            # Inyectar Metadata (Origen del dato)
                            for item in results:
                                if isinstance(item, dict):
                                    item['_scrape_coords'] = coords
                                    item['_category'] = query
                            
                            cell_data.extend(results)
                            print(f"{len(results)}", end="")
                            
                            # Si devuelve menos de 20, es la última página
                            if len(results) < 20: break
                            
                            page += 1
                            time.sleep(0.5) # Respeto a la API
                        else:
                            print(f"[Err{r.status_code}]", end="")
                            time.sleep(2)
                            break # Si falla la API, pasamos a siguiente categoría
                            
                    except Exception as e:
                        print(f"[NetErr]", end="")
                        time.sleep(2)
                        break
                print(" ok.")

            # --- GUARDADO TRANSACCIONAL (CADA CELDA) ---
            # Guardamos tras acabar todas las categorías de UNA celda
            all_data.extend(cell_data)
            completed_cells.append(coords)
            
            # 1. Guardamos DATOS (Sobrescribiendo el archivo con la lista ampliada)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            
            # 2. Guardamos ESTADO (Checkpoint)
            with open(checkpoint_file, 'w') as f:
                json.dump({"completed_cells": completed_cells}, f)

    except KeyboardInterrupt:
        print("\n🛑 DETENIDO POR USUARIO. El progreso está guardado.")
        print("   Puedes reanudar cuando quieras ejecutando de nuevo.")
        return

    print(f"\n✅ CIUDAD COMPLETADA: {city_name}")
    print(f"📊 Total locales: {len(all_data)}")
    print(f"💸 Peticiones esta sesión: {total_requests_session}")
    
    # Limpieza final: Borrar checkpoint solo si acabó TODAS las celdas
    if len(completed_cells) == len(grid):
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print("🧹 Checkpoint borrado (trabajo terminado).")

if __name__ == "__main__":
    # ¡CONFIGURACIÓN DE LANZAMIENTO!
    
    # 1. Asegúrate de haber ejecutado '00_generate_postgis_grid.py' si quieres el ahorro.
    # 2. Lanza Madrid:
    run_scanner("MADRID")
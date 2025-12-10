import sys
import os
import time

# --- CORRECCIÓN DE RUTAS ---
# 1. Obtenemos la ruta de ESTE fichero (.../spatia-li/ingest/30_scrape_google_poi.py)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Obtenemos la carpeta PADRE (.../spatia-li)
parent_dir = os.path.dirname(current_dir)

# 3. Añadimos la carpeta padre al "path" de Python para que encuentre utils.py
sys.path.append(parent_dir)

# Ahora sí funcionará el import
from utils import log_execution

# --- EL PROCESO (Task ID 30) ---
@log_execution(task_id=30)
def update_retail_pois(city):
    
    if not city or city == "GLOBAL":
        raise ValueError("❌ Error: Este proceso requiere especificar una ciudad. Ej: 'MADRID'")

    print(f"🏙️ Iniciando actualización de POIs para: {city}")
    # Simulación rápida
    time.sleep(1) 
    print(f"   ✅ Datos para {city} procesados correctamente.")

    return f"Update finalizado. POIs de {city} sincronizados."

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_city = sys.argv[1]
    else:
        target_city = None

    update_retail_pois(city=target_city)
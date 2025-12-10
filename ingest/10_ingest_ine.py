import sys
import os
import glob
import re

# ==========================================
# 1. AJUSTE DE RUTAS
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) # Subimos 1 nivel (de ingest a spatia-li)
sys.path.append(project_root)

from utils import log_execution
try:
    from config import DATA_DIR
except ImportError:
    DATA_DIR = "data/raw"

# ==========================================
# 2. DOCUMENTACIÓN DEL PROCESO
# ==========================================
INE_URL = "https://www.ine.es/dynt3/inebase/index.htm?padre=12385&capsel=12384"
INSTRUCCIONES = f"""
    🛑 ESTE ES UN PROCESO MANUAL (Frecuencia: Anual)
    
    1. Entra en la web del INE: 
       {INE_URL}
       
    2. Busca "Indicadores de renta media y mediana" > "Resultados por secciones censales".
    3. Descarga el fichero CSV (separado por punto y coma).
    4. Guárdalo en: {os.path.join(project_root, DATA_DIR)}
    5. Renómbralo siguiendo este patrón: 'INE_YYYY_Renta.csv' (Ej: INE_2024_Renta.csv)
"""

# ==========================================
# 3. PROCESO DE VERIFICACIÓN
# ==========================================
@log_execution(task_id=10)
def manual_ingest_verification(scope="GLOBAL_RELEASE"):
    
    # Si es un parche local, simplemente damos el OK
    if scope != "GLOBAL_RELEASE":
        return f"Parche Manual ({scope}): Se asume que los datos locales están listos."

    print("📋 Verificando Ingesta Manual de Datos INE...")
    print(INSTRUCCIONES)
    
    raw_path = os.path.join(project_root, DATA_DIR)
    
    # Buscamos cualquier archivo que cumpla el patrón INE_xxxx_Renta.csv
    pattern = os.path.join(raw_path, "INE_*_Renta.csv")
    files = glob.glob(pattern)
    
    if not files:
        error_msg = (
            "❌ ERROR: No encuentro el fichero descargado.\n"
            f"Esperaba algo como: {pattern}\n"
            "Por favor, descarga el fichero manualmente y vuelve a ejecutar este script."
        )
        raise FileNotFoundError(error_msg)
    
    # Cogemos el último por si hay varios
    files.sort()
    latest_file = files[-1]
    filename = os.path.basename(latest_file)
    
    # Extraemos el año del nombre del fichero
    year_match = re.search(r'20\d{2}', filename)
    year = year_match.group(0) if year_match else "Desconocido"
    
    print(f"✅ Archivo encontrado: {filename}")
    print(f"📂 Ruta: {latest_file}")
    
    return f"Verificación Exitosa. Datos INE ({year}) disponibles en disco."

# ==========================================
# 4. ENTRY POINT
# ==========================================
if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "GLOBAL_RELEASE"
    manual_ingest_verification(scope=arg)
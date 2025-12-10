# config.py

# --- 1. PROYECTO ---
# Lista técnica para OSMnx (necesita "Ciudad, Pais")
TARGET_CITIES = [
    "Madrid, Spain"
]

# Lista corta para tu Panel de Control y Scopes (usada en admin_ops.py)
# ESTA es la que lee la App para generar las filas de tareas
ACTIVE_CITIES = ["MADRID", "VALENCIA", "BARCELONA"]

# --- 2. PARÁMETROS TÉCNICOS ---
H3_RESOLUTION = 9
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# ✅ ALIAS DE COMPATIBILIDAD
# Esto hace que mis scripts (que buscan DB_CONNECTION_STR) funcionen
# leyendo tu variable original DB_URL.
DB_CONNECTION_STR = DB_URL 

# --- 3. RUTAS DE DATOS ---
DATA_DIR = "data/raw"
OSRM_WALK_URL = "http://localhost:5001"

# --- 4. PESOS DE NEGOCIO ---
FEATURE_WEIGHTS = {
    'income_smooth_score': 4.0,
    'target_pop_smooth_score': 3.0,
    'dist_cafe_score': 1.0,
    'dist_gym_score': 1.0,
    'dist_shop_score': 1.0
}
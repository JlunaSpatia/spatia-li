# config.py

# --- 1. PROYECTO ---
# Lista de ciudades a procesar.
# Formato: "NombreCiudad, País" (para que OSMnx no se confunda con Córdoba, Argentina)
TARGET_CITIES = [
    "Madrid, Spain",
    "Valencia, Spain" 
]

# --- 2. PARÁMETROS TÉCNICOS ---
H3_RESOLUTION = 9
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# --- 3. RUTAS DE DATOS ---
# Rutas relativas para que funcione en cualquier PC
DATA_DIR = "data/raw"
OSRM_WALK_URL = "http://localhost:5001"

# --- 4. PESOS DE NEGOCIO (EL CEREBRO) ---
# Puedes ajustar la estrategia aquí sin tocar el código
FEATURE_WEIGHTS = {
    'income_smooth_score': 4.0,
    'target_pop_smooth_score': 3.0,
    'dist_cafe_score': 1.0,
    'dist_gym_score': 1.0,
    'dist_shop_score': 1.0
}
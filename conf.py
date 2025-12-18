import os

# ==========================================
# 1. DATABASE & CONECTIVIDAD
# ==========================================
# Tu URL de conexión principal
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# ✅ ALIAS DE COMPATIBILIDAD
# Mantenemos esto para que los scripts antiguos y nuevos funcionen igual
DB_CONNECTION_STR = DB_URL 

# Servidor OSRM local (Docker)
OSRM_WALK_URL = "http://localhost:5001"

# ==========================================
# 2. PARÁMETROS TÉCNICOS Y RUTAS
# ==========================================
H3_RESOLUTION = 9
DATA_DIR = "data/raw"

# ==========================================
# 3. DEFINICIÓN DE ZONAS (BOUNDING BOXES)
# ==========================================
# Coordenadas exactas para evitar errores de OSMnx y zonas vacías
CITY_BBOXES = {
    "MADRID": {
        "min_lat": 40.3200, "max_lat": 40.5600, 
        "min_lon": -3.8000, "max_lon": -3.5200 
    },
    "VALENCIA": {
        "min_lat": 39.3800, "max_lat": 39.5600,
        "min_lon": -0.4800, "max_lon": -0.2800
    },
    # BARCELONA DEFINITIVA (Aeropuerto + Vallès + Molins + Costa Badalona)
    "BARCELONA": {
        "min_lat": 41.2800, 
        "max_lat": 41.5000, 
        "min_lon": 1.9800, 
        "max_lon": 2.2900 
    },
    "LASTABLAS_REAL": { # Zona de test
        "min_lat": 40.4980, "max_lat": 40.5220, 
        "min_lon": -3.6800, "max_lon": -3.6550 
    },

    "SORIA_TEST": {
        "min_lat": 41.7500, 
        "max_lat": 41.7800,
        "min_lon": -2.4900, 
        "max_lon": -2.4500
    },
    "EL_PORTIL": {
        "min_lat": 37.2125,  # Sur
        "max_lat": 37.2185,  # Norte
        "min_lon": -7.0460,  # Oeste
        "max_lon": -7.0380   # Este
    },
    "ALCALA": {
        "min_lat": 40.4580,  # Sur (Barrio Venecia / Nueva Alcalá)
        "max_lat": 40.5300,  # Norte (El Ensanche / Espartales)
        "min_lon": -3.4300,  # Oeste (La Garena)
        "max_lon": -3.3200   # Este (Universidad / Hospital)
    }
}

# ==========================================
# 4. PANEL DE CONTROL (FILTRO DE EJECUCIÓN)
# ==========================================
# Si está VACÍO [], los scripts procesarán TODAS las ciudades del diccionario.
# Si tiene nombres ["BARCELONA"], SOLO se procesará esa ciudad.
ACTIVE_CITIES = ["BARCELONA"] 

# ==========================================
# 5. PESOS DE NEGOCIO (SCORING)
# ==========================================
# Usados en el script de puntuación final (paso 3 o 4)
FEATURE_WEIGHTS = {
    'income_smooth_score': 4.0,     # Prioridad Alta: Renta
    'target_pop_smooth_score': 3.0, # Prioridad Alta: Población objetivo
    'dist_cafe_score': 1.0,         # Sinergia
    'dist_gym_score': 1.0,          # Competencia/Sinergia
    'dist_shop_score': 1.0          # Tráfico peatonal
}
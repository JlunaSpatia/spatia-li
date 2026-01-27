# market_scanner/conf.py
import os

# ==========================================
# 🌉 PUENTE DE CONEXIÓN (WSL -> DOCKER WINDOWS)
# ==========================================
def get_windows_host_ip():
    """
    Detecta la IP del anfitrión (Windows) desde WSL leyendo /etc/resolv.conf
    """
    try:
        with open("/etc/resolv.conf", "r") as f:
            for line in f:
                if "nameserver" in line:
                    return line.split()[1]
    except:
        pass
    return "host.docker.internal" # Fallback para Docker

# 1. Detectamos la IP
WIN_HOST = get_windows_host_ip()

# 2. Definimos el Puerto (SEGÚN TU CAPTURA DE DOCKER ES EL 5433)
DB_PORT = "5433"

print(f"⚙️  Conectando a Docker en Windows -> IP: {WIN_HOST} | Puerto: {DB_PORT}")

# 3. Construimos la cadena de conexión
# Usuario: postgres | Pass: postgres | DB: spatia
DB_CONNECTION_STR = f"postgresql://postgres:postgres@{WIN_HOST}:{DB_PORT}/spatia"


# ==========================================
# 🔑 API KEYS & PARÁMETROS
# ==========================================
SCRAPINGDOG_API_KEY = "696407ecbc71bda154ab82dd"

GRID_STEP = 0.010
ZOOM_LEVEL = "16z" # Nivel de barrio detallado


# ==========================================
# 🌍 CIUDADES (BOUNDING BOXES) - COMPLETO
# ==========================================
CITY_BBOXES = {
    # 1. GRAN MADRID
    "MADRID": {
        "min_lat": 40.3200, 
        "max_lat": 40.5600, 
        "min_lon": -3.8000, 
        "max_lon": -3.5200  
    },
    # 2. GRAN VALENCIA
    "VALENCIA": {
        "min_lat": 39.3800, 
        "max_lat": 39.5600,
        "min_lon": -0.4800, 
        "max_lon": -0.2800
    },
    # 3. ZONA DE TEST (Las Tablas)
    "LASTABLAS_REAL": {
        "min_lat": 40.4980,  
        "max_lat": 40.5220,  
        "min_lon": -3.6800, 
        "max_lon": -3.6550   
    },
    # 4. GRAN BARCELONA
    "BARCELONA": {
        "min_lat": 41.2800, 
        "max_lat": 41.5000, 
        "min_lon": 1.9800,  
        "max_lon": 2.2900    
    },
    # 5. ALCALÁ DE HENARES
    "ALCALA": {
        "min_lat": 40.4580,  # Sur 
        "max_lat": 40.5300,  # Norte 
        "min_lon": -3.4300,  # Oeste 
        "max_lon": -3.3200   # Este 
    },
    # 6. EL PORTIL (Huelva) - Tu zona de test actual
    "EL_PORTIL": {
        "min_lat": 37.2125, 
        "max_lat": 37.2185, 
        "min_lon": -7.0460, 
        "max_lon": -7.0380  
    }
}


# ==========================================
# 🛍️ CATEGORÍAS (LEGACY)
# ==========================================
# Se mantienen por compatibilidad, aunque el MVP use el CSV.
CATEGORIAS = [
    # GRUPO A: TRÁFICO RECURRENTE
    "Supermercado", "Panadería", "Farmacia",          
    # GRUPO B: HOSTELERÍA & OCIO
    "Restaurante", "Bar", "Cafetería", "Comida rápida",     
    # GRUPO C: DESTINO DE COMPRA & LIFESTYLE
    "Tienda de ropa", "Tienda de deportes", "Gimnasio",          
    # GRUPO D: EL GRAN IMÁN
    "Centro comercial"   
]

# 6. --- CIUDADES ACTIVAS ---
ACTIVE_CITIES = ["MADRID"]
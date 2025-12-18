# market_scanner/config.py

# 1. --- CONEXIÓN A BASE DE DATOS (¡ESTO ES LO QUE FALTABA!) ---
# Usuario: postgres, Contraseña: postgres, Puerto: 5432, Base de datos: spatia
DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

# 2. --- API KEYS ---
SCRAPINGDOG_API_KEY = "69368e8607da3da240a81a4f"

# 3. --- PARÁMETROS DE ESCANEO ---
GRID_STEP = 0.010
ZOOM_LEVEL = "16z"

# 4. --- CIUDADES (Bounding Boxes) ---
# Nota: He renombrado 'CITIES' a 'CITY_BBOXES' para mantener consistencia con los otros scripts.
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
    "ALCALA": {
        "min_lat": 40.4580,  # Sur (Barrio Venecia / Nueva Alcalá)
        "max_lat": 40.5300,  # Norte (El Ensanche / Espartales)
        "min_lon": -3.4300,  # Oeste (La Garena)
        "max_lon": -3.3200   # Este (Universidad / Hospital)
    },

    
    # 5. EL PORTIL (Huelva) - Tu zona de test actual
    "EL_PORTIL": {
        "min_lat": 37.2125, 
        "max_lat": 37.2185, 
        "min_lon": -7.0460, 
        "max_lon": -7.0380  
    }
}

# 5. --- CATEGORÍAS (Lifestyle & Retail) ---
CATEGORIAS = [
    # GRUPO A: TRÁFICO RECURRENTE
    "Supermercado",      
    "Panadería",         
    "Farmacia",          

    # GRUPO B: HOSTELERÍA & OCIO
    "Restaurante",       
    "Bar",               
    "Cafetería",         
    "Comida rápida",     

    # GRUPO C: DESTINO DE COMPRA & LIFESTYLE
    "Tienda de ropa",    
    "Tienda de deportes", 
    "Gimnasio",          
    
    # GRUPO D: EL GRAN IMÁN
    "Centro comercial"   
]

# 6. --- CIUDADES ACTIVAS ---
# Esto define qué ciudad procesan los scripts por defecto
ACTIVE_CITIES = ["MADRID"]
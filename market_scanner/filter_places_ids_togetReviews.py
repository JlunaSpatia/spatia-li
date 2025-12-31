import sys
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from conf import DB_URL
except ImportError:
    print("⚠️ Usando DB default local.")
    DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# Configuración de salida
TARGET_CITY = "El Portil"  # Nombre tal cual aparece en la columna city_source de la BBDD
CARPETA_SALIDA = os.path.join(BASE_DIR, 'data', 'processed', 'gyms')
if not os.path.exists(CARPETA_SALIDA):
    os.makedirs(CARPETA_SALIDA)

FICHERO_SALIDA = os.path.join(CARPETA_SALIDA, f'{TARGET_CITY.upper().replace(" ", "_")}_GYMS_FROM_DB.json')

# ==========================================
# 2. CATEGORÍAS (Tu lógica de filtrado)
# ==========================================
CAT_DIRECTA = {
    "gimnasio", "centro de gimnasia", "centro deportivo", 
    "club deportivo", "polideportivo", "instalaciones deportivas",
    "centro de salud y bienestar", "complejo deportivo", "fitness center"
}

CAT_ESPECIALIZADA = {
    "entrenador personal", "centro de pilates", "centro de yoga", 
    "gimnasio con rocódromo", "escalada", "centro de natación",
    "estudio de yoga", "estudio de pilates", "crossfit"
}

CAT_COMBATE = {
    "escuela de boxeo", "gimnasio de boxeo muay thai", 
    "escuela de kickboxing", "escuela de artes marciales", 
    "escuela de kung-fu", "escuela de taekwondo", 
    "escuela de jiu-jitsu", "escuela de judo", 
    "escuela de defensa personal", "escuela deportiva",
    "club de judo", "club de karate"
}

def obtener_gyms_desde_db():
    engine = create_engine(DB_URL)
    
    print(f"--- 📡 CONECTANDO A BASE DE DATOS PARA CIUDAD: {TARGET_CITY} ---")

    # 1. Traemos todo lo de esa ciudad (es más rápido filtrar en Python por los sets)
    #    Traemos específicamente el reviews_link y otros campos útiles.
    query = text("""
        SELECT 
            place_id, 
            name, 
            category, 
            rating, 
            reviews, 
            address, 
            reviews_link,
            lat, 
            lng
        FROM core.pois 
        WHERE city_source = :city
    """)

    gyms_encontrados = []
    stats = {"Procesados": 0, "Directa": 0, "Especializada": 0, "Combate": 0, "Otros": 0}

    with engine.connect() as conn:
        # Usamos pandas para leer eficientemente
        print(" ⏳ Ejecutando query...")
        df = pd.read_sql(query, conn, params={"city": TARGET_CITY})
        
        print(f" 📥 Analizando {len(df)} registros descargados...")

        for _, row in df.iterrows():
            stats["Procesados"] += 1
            
            # Limpieza de categoría
            raw_cat = row['category']
            if not raw_cat:
                continue
            
            cat_limpia = str(raw_cat).strip().lower()

            # Clasificación
            tipo = None
            if cat_limpia in CAT_DIRECTA:
                tipo = "COMPETENCIA_DIRECTA"
                stats["Directa"] += 1
            elif cat_limpia in CAT_ESPECIALIZADA:
                tipo = "COMPETENCIA_ESPECIALIZADA"
                stats["Especializada"] += 1
            elif cat_limpia in CAT_COMBATE:
                tipo = "COMPETENCIA_COMBATE"
                stats["Combate"] += 1
            else:
                stats["Otros"] += 1

            if tipo:
                # Construimos el diccionario de salida
                item = {
                    'place_id': row['place_id'],
                    'name': row['name'],
                    'category': row['category'], # Guardamos la original para referencia
                    'category_clean': cat_limpia,
                    'rating': row['rating'],
                    'reviews': row['reviews'],
                    'address': row['address'],
                    'gps_coordinates': { # Mantenemos formato compatible con tools anteriores
                        'latitude': row['lat'],
                        'longitude': row['lng']
                    },
                    'reviews_link': row['reviews_link'], # <--- AQUÍ ESTÁ EL DATO CLAVE
                    'analysis_type': tipo,
                    'city_source': TARGET_CITY
                }
                gyms_encontrados.append(item)

    # ==========================================
    # 3. RESULTADOS Y GUARDADO
    # ==========================================
    print("\n--- ESTADÍSTICAS DEL FILTRADO ---")
    print(f"Total registros en DB ({TARGET_CITY}): {stats['Procesados']}")
    print(f"✅ Gimnasios Directos: {stats['Directa']}")
    print(f"✅ Especializados: {stats['Especializada']}")
    print(f"✅ Combate: {stats['Combate']}")
    print(f"❌ Descartados: {stats['Otros']}")

    if gyms_encontrados:
        with open(FICHERO_SALIDA, 'w', encoding='utf-8') as f:
            json.dump(gyms_encontrados, f, ensure_ascii=False, indent=4)
        print(f"\n📁 Fichero generado: {FICHERO_SALIDA}")
        print(f"🔢 Total gimnasios exportados: {len(gyms_encontrados)}")
    else:
        print("\n⚠️ No se encontraron gimnasios con los filtros actuales.")

if __name__ == "__main__":
    obtener_gyms_desde_db()
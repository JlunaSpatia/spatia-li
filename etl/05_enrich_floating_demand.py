import osmnx as ox
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings
import os
import sys
import numpy as np 

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config import DB_CONNECTION_STR, H3_RESOLUTION
except ImportError:
    # Fallback por si acaso
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"
    H3_RESOLUTION = 9

warnings.filterwarnings("ignore")

# --- 1. LISTAS DE ORO (AFFINITY BRANDS) ---
# Si el nombre del local contiene alguna de estas palabras, el score se dispara.
AFFINITY_BRANDS = [
    # Tu lista deseada
    "scalpers", "rituals", "mango", "nude project", 
    # Añadidos estratégicos para Blue Banana
    "brownie", "bimba y lola", "hoff", "pompeii", "alo yoga", "lululemon",
    "apple store", "starbucks", "goiko", "honest greens", "zara", "massimo dutti"
]

# --- 2. CLASIFICACIÓN SEMÁNTICA (BAG OF WORDS) ---
KEYWORDS_HIPSTER = [
    "vintage", "pilates", "yoga", "vegano", "vegetariano", "brunch", "artesanal", 
    "burbujas", "te", "cafetería", "perros", "surf", "skate", "tatuaje", "diseno",
    "co-working", "coworking", "eco", "bio", "sushi", "poke", "ramen", "izakaya",
    "bocatería", "tostadores", "matcha", "açaí"
]

KEYWORDS_HEALTH = [
    "gimnasio", "crossfit", "boxeo", "artes marciales", "entrenador", "deportivo", 
    "fitness", "rocódromo", "natación", "tennis", "padel", "golf", "bicicleta",
    "nutrición", "fisioterapia", "cycle"
]

KEYWORDS_RETAIL = [
    "boutique", "joyería", "relojería", "moda", "zapatería", "vestidos", "trajes",
    "sastre", "confección", "lencería", "regalos", "floristería", "librería",
    "muebles", "decoración", "showroom"
]

KEYWORDS_NIGHT = [
    "bar", "pub", "club", "discoteca", "coctelería", "vinoteca", "cervecería", 
    "teatro", "cine", "conciertos", "museo", "galería", "karaoke", "rooftop"
]

def classify_poi_logic(row):
    """
    Analiza nombre y tipo para asignar categoría y peso.
    Prioridad: AFFINITY BRANDS > HIPSTER > HEALTH > RETAIL > NIGHT
    """
    # Convertimos a minúsculas y string para evitar errores con nulos
    name = str(row['name']).lower()
    main_type = str(row['main_type']).lower()
    
    # 1. AFFINITY BRAND CHECK (El Bonus "VIP")
    # Si es una marca afín, va directo a RETAIL o HIPSTER con peso GIGANTE
    for brand in AFFINITY_BRANDS:
        if brand in name:
            # Si es comida (Goiko, Starbucks) lo mandamos a Hipster, si es ropa a Retail
            if any(x in main_type for x in ["restaurante", "cafe", "comida"]):
                 return "score_hipster", 300, True # 300 Puntos base + Flag Affinity
            return "score_retail", 300, True

    # 2. HIPSTER
    if any(k in main_type for k in KEYWORDS_HIPSTER): return "score_hipster", 100, False
    
    # 3. HEALTH
    if any(k in main_type for k in KEYWORDS_HEALTH): return "score_health", 80, False
        
    # 4. RETAIL PREMIUM
    if any(k in main_type for k in KEYWORDS_RETAIL): return "score_retail", 60, False
        
    # 5. NIGHTLIFE
    if any(k in main_type for k in KEYWORDS_NIGHT): return "score_night", 70, False
        
    return "score_generic", 10, False

def get_google_pois_affinity(engine, city):
    print(f"      🧠 Descargando POIs (con detección de Marcas VIP) de {city}...")
    
    # CORRECCIÓN AQUÍ: Usamos 'title as name' para que Python reciba una columna 'name'
    query = f"""
    SELECT latitude, longitude, title as name, main_type, rating, reviews_count, price_level
    FROM public.retail_poi_master
    WHERE city = '{city.upper()}' 
      AND snapshot_date = (SELECT MAX(snapshot_date) FROM public.retail_poi_master WHERE city = '{city.upper()}')
      AND main_type IS NOT NULL
    """
    try:
        df = pd.read_sql(query, engine)
        if df.empty: return pd.DataFrame()
        
        # --- LIMPIEZA Y CONVERSIÓN DE TIPOS (BLINDAJE) ---
        df['name'] = df['name'].fillna('')
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(3.0)
        df['reviews_count'] = pd.to_numeric(df['reviews_count'], errors='coerce').fillna(0)
        df['price_level'] = pd.to_numeric(df['price_level'], errors='coerce').fillna(1)
        
        # --- CLASIFICACIÓN AVANZADA ---
        # Aplicamos la lógica fila a fila
        classification = df.apply(classify_poi_logic, axis=1, result_type='expand')
        df.rename(columns={0: 'category', 1: 'base_weight', 2: 'is_affinity'}, inplace=True)
        df['category'] = classification[0]
        df['base_weight'] = classification[1]
        df['is_affinity'] = classification[2]
        
        # --- CÁLCULO FINAL DE PUNTUACIÓN ---
        # Logaritmo para suavizar popularidad (10 reviews = 1, 100 reviews = 2)
        df['log_reviews'] = np.log10(df['reviews_count'] + 1)
        
        # Factor Precio Inteligente: 
        # Si es Affinity Brand O tiene precio alto, multiplicamos x1.5
        df['price_factor'] = df.apply(
            lambda x: 1.5 if (x['price_level'] >= 3 or x['is_affinity']) else 1.0, 
            axis=1
        )
        
        df['final_score'] = (
            df['base_weight'] * (df['rating'] / 5.0) * df['log_reviews'] *
            df['price_factor']
        )
        
        # Feedback en consola si encontramos marcas top (solo para ver que funciona)
        vip_found = df[df['is_affinity'] == True]
        if not vip_found.empty:
            print(f"         💎 ¡BOOM! Encontradas {len(vip_found)} marcas Affinity (Ej: {vip_found.iloc[0]['name']})")

        # Filtramos basura genérica
        return df[df['category'] != 'score_generic']

    except Exception as e:
        print(f"      ❌ Error SQL/Pandas en {city}: {e}")
        return pd.DataFrame()

def enrich_activity_affinity():
    print("🌊 PASO 05: MODELANDO ACTIVIDAD (VIBE + AFFINITY BRANDS)...")
    engine = create_engine(DB_CONNECTION_STR)

    # 1. LEER HEXÁGONOS MAESTROS
    print("   Leyendo zonas hexagonales...")
    try:
        sql = "SELECT h3_index, city, geometry FROM retail_hexagons_enriched"
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    except:
        sql = "SELECT h3_index, city, geometry FROM retail_hexagons"
        gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    cities = gdf_hex['city'].unique()
    
    # Inicializamos columnas de score
    score_cols = ['score_hipster', 'score_health', 'score_retail', 'score_night', 'gravity_score']
    for col in score_cols:
        gdf_hex[col] = 0.0
    
    # Indexamos por H3 para asignación rápida
    gdf_hex.set_index('h3_index', inplace=True)

    for city in cities:
        clean_city = city.split(",")[0]
        print(f"   🧲 Analizando {clean_city}...")

        # A. INFRAESTRUCTURA (OSM)
        try:
            area = ox.geocode_to_gdf(city).geometry[0]
            tags = {"railway": ["subway_entrance"], "amenity": ["university"]}
            pois_osm = ox.features_from_polygon(area, tags=tags)
            if not pois_osm.empty:
                pois_osm['geometry'] = pois_osm.geometry.centroid
                pois_osm['h3'] = pois_osm.apply(lambda x: h3.geo_to_h3(x.geometry.y, x.geometry.x, H3_RESOLUTION), axis=1)
                
                counts = pois_osm.groupby('h3').size() * 300
                
                # FILTRO DE SEGURIDAD (Ignorar puntos fuera del mapa)
                valid_indices = counts.index.intersection(gdf_hex.index)
                if not valid_indices.empty:
                    gdf_hex.loc[valid_indices, 'gravity_score'] += counts.loc[valid_indices]
                    print(f"      🚇 Infraestructura OSM integrada ({len(valid_indices)} zonas).")
        except: pass

        # B. LIFESTYLE (GOOGLE + AFFINITY)
        df_google = get_google_pois_affinity(engine, clean_city)
        
        if not df_google.empty:
            df_google['h3'] = df_google.apply(lambda x: h3.geo_to_h3(x['latitude'], x['longitude'], H3_RESOLUTION), axis=1)
            
            # Agrupar sumando scores por hexágono y categoría
            grouped = df_google.groupby(['h3', 'category'])['final_score'].sum().unstack(fill_value=0)
            
            # FILTRO DE SEGURIDAD CRÍTICO
            valid_h3 = grouped.index.intersection(gdf_hex.index)
            grouped_clean = grouped.loc[valid_h3]
            
            if not grouped_clean.empty:
                # 1. Sumar cada score específico (Hipster, Retail, etc)
                for col in grouped_clean.columns:
                    if col in gdf_hex.columns:
                        gdf_hex.loc[valid_h3, col] = gdf_hex.loc[valid_h3, col].add(grouped_clean[col], fill_value=0)
                
                # 2. Sumar al Score de Gravedad Total
                total_activity = grouped_clean.sum(axis=1)
                gdf_hex.loc[valid_h3, 'gravity_score'] += total_activity
                
                print(f"      ✅ Integrados POIs en {len(valid_h3)} hexágonos.")
            else:
                print("      ⚠️ AVISO: Se encontraron POIs pero ninguno cae dentro de los hexágonos (¿Coordenadas desplazadas?).")

    # GUARDADO FINAL
    print("💾 Guardando Vibe Scores en Base de Datos...")
    df_final = gdf_hex.reset_index()[['h3_index'] + score_cols]
    df_final.to_sql('temp_scores', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        # Aseguramos que existe la tabla destino
        conn.execute(text("CREATE TABLE IF NOT EXISTS retail_hexagons_enriched AS SELECT * FROM retail_hexagons WHERE 1=0;"))
        
        # Añadimos columnas si faltan
        for col in score_cols:
            conn.execute(text(f"ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS {col} FLOAT;"))
        
        # Update Dinámico
        set_clause = ", ".join([f"{col} = s.{col}" for col in score_cols])
        
        conn.execute(text(f"""
            UPDATE retail_hexagons_enriched AS m
            SET {set_clause}
            FROM temp_scores AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_scores;"))

    print("✅ MODELO DE ACTIVIDAD COMPLETADO.")
    
    # Check del campeón
    top = df_final.sort_values('gravity_score', ascending=False).head(1)
    if not top.empty:
        print(f"   🏆 Zona más potente: {top.iloc[0]['h3_index']} (Score Total: {top.iloc[0]['gravity_score']:.0f})")

if __name__ == "__main__":
    enrich_activity_affinity()
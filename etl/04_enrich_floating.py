import osmnx as ox
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import h3
import warnings

# --- CONFIG ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
H3_RES = 9
warnings.filterwarnings("ignore")

# NUEVOS FILTROS "PREMIUM"
# Intentamos ser más selectivos con las etiquetas
GENERATORS = {
    # UNIVERSIDADES: Le bajamos el peso si no podemos distinguir pública/privada, 
    # pero confiamos en que el filtro de renta posterior limpie las "malas".
    "university": {"tags": {"amenity": "university"}, "weight": 400},
    
    # METRO: Sigue siendo clave para tráfico
    "transit":    {"tags": {"railway": ["subway_entrance", "tram_stop"]}, "weight": 300},
    
    # OFICINAS: Gente con nómina
    "office":     {"tags": {"office": True}, "weight": 100},
    
    # COMPETENCIA DIRECTA (MODA): Esto valida la zona comercial
    "fashion":    {"tags": {"shop": "clothes"}, "weight": 200},
    
    # LIFESTYLE (Intentamos filtrar "Bar Manolo" vs "Sitio Cool")
    # Fitness centers suelen ser gimnasios de pago (buen proxy)
    "fitness":    {"tags": {"leisure": "fitness_centre", "sport": "crossfit"}, "weight": 150},
    
    # CAFÉ: Usamos 'coffee_shop' que a veces filtra mejor que el genérico, 
    # y añadimos 'coworking' que es imán de nómadas digitales (target BB)
    "hipster_hub": {"tags": {"cuisine": "coffee_shop", "amenity": "coworking_space"}, "weight": 250}
}

def enrich_activity_refined():
    print("🌊 PASO 04: MODELANDO ACTIVIDAD (FILTRO PREMIUM)...")
    engine = create_engine(DB_URL)

    print("   Leyendo retail_hexagons_enriched...")
    sql = "SELECT h3_index, city, geometry FROM retail_hexagons_enriched"
    gdf_hex = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    cities = gdf_hex['city'].unique()
    
    # Reiniciamos scores
    hex_scores = pd.DataFrame({'h3_index': gdf_hex['h3_index'], 'gravity_score': 0}).set_index('h3_index')

    for city in cities:
        print(f"   🧲 Analizando imanes en {city}...")
        try:
            area = ox.geocode_to_gdf(f"{city}, Spain").geometry[0]
            
            for gen_type, config in GENERATORS.items():
                try:
                    pois = ox.features_from_polygon(area, tags=config['tags'])
                    if pois.empty: continue
                    
                    pois['geometry'] = pois.geometry.centroid
                    pois['h3_index'] = pois.apply(lambda x: h3.geo_to_h3(x.geometry.y, x.geometry.x, H3_RES), axis=1)
                    
                    counts = pois.groupby('h3_index').size()
                    score_update = counts * config['weight']
                    
                    hex_scores['gravity_score'] = hex_scores['gravity_score'].add(score_update, fill_value=0)
                    print(f"      -> {len(pois)} {gen_type}s encontrados.")
                except: continue
        except Exception as e: 
            print(f"   ❌ Error en {city}: {e}")

    print("💾 Guardando Score de Gravedad...")
    df_final = hex_scores.reset_index()
    df_final.to_sql('temp_gravity', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE retail_hexagons_enriched ADD COLUMN IF NOT EXISTS gravity_score FLOAT;"))
        conn.execute(text("""
            UPDATE retail_hexagons_enriched AS m
            SET gravity_score = COALESCE(s.gravity_score, 0)
            FROM temp_gravity AS s
            WHERE m.h3_index = s.h3_index;
        """))
        conn.execute(text("DROP TABLE temp_gravity;"))
        conn.commit()

    print("✅ ACTIVIDAD REFINADA INTEGRADA.")

if __name__ == "__main__":
    enrich_activity_refined()
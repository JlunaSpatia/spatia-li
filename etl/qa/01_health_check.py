import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
import numpy as np

# ==========================================
# 1. SETUP
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
# Subimos 2 niveles: qa -> processes -> spatia-li
project_root = os.path.dirname(os.path.dirname(current_dir)) 
sys.path.append(project_root)

try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

# ==========================================
# 2. MOTOR DE DIAGNÓSTICO
# ==========================================
def analyze_city(city_name, df_city):
    """Analiza los datos de una ciudad específica y devuelve una lista de problemas"""
    issues = []
    warnings = []
    
    total_hex = len(df_city)
    
    # --- A. INTEGRIDAD DE DATOS ---
    # 1. Nulos en Renta
    null_income = df_city['avg_income'].isnull().sum()
    if null_income > 0:
        pct = (null_income / total_hex) * 100
        issues.append(f"❌ DATA GAP: {null_income} hexágonos ({pct:.1f}%) no tienen Renta (NULL). Fallo cruce INE.")

    # 2. Nulos en Distancias (Fallo Scraping)
    if 'dist_cafe' in df_city.columns:
        null_dist = df_city['dist_cafe'].isnull().sum()
        if null_dist > 0:
            issues.append(f"❌ SCRAPING GAP: {null_dist} zonas no tienen datos de distancia a POIs.")

    # --- B. COHERENCIA DE NEGOCIO ---
    # 3. Falsos Pobres (Gente sin dinero)
    # Zonas con población joven relevante (>50) pero Renta <= 0
    ghost_poor = df_city[(df_city['target_pop'] > 50) & (df_city['avg_income'] <= 0)]
    if len(ghost_poor) > 0:
        issues.append(f"❌ FALSOS POBRES: {len(ghost_poor)} zonas habitadas tienen Renta 0. (Revisar cobertura CUSEC INE).")

    # 4. Old Money / Oficinas (Renta alta, sin jóvenes)
    # Esto es un aviso, no un error.
    old_money = df_city[(df_city['avg_income'] > 65000) & (df_city['target_pop'] < 5)]
    if len(old_money) > 0:
        warnings.append(f"⚠️ OLD MONEY/OFICINAS: {len(old_money)} zonas muy ricas (>65k) sin público joven.")

    # --- C. DISTRIBUCIÓN ---
    # 5. Distancias sospechosas
    # Si la media de distancia a un café en la ciudad es > 5km, algo va mal con el scraping
    mean_dist_cafe = df_city['dist_cafe'].mean() if 'dist_cafe' in df_city.columns else 0
    if mean_dist_cafe > 5000:
        warnings.append(f"⚠️ SCRAPING DUDOSO: La distancia media a un café es muy alta ({mean_dist_cafe/1000:.1f} km). ¿Faltan datos en Google?")

    return issues, warnings

def run_health_check():
    print("🩺 INICIANDO DIAGNÓSTICO POR CIUDAD (QA V3)...")
    engine = create_engine(DB_CONNECTION_STR)
    
    print("   📥 Cargando datos maestros...")
    # Cargamos TODO, pero ordenado por ciudad
    query = "SELECT h3_index, city, avg_income, target_pop, dist_cafe FROM retail_hexagons_enriched"
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Error fatal conectando a BBDD: {e}")
        return

    # Normalizar nombres de ciudad (Mayúsculas y trim)
    if 'city' not in df.columns:
        print("❌ Error: La tabla no tiene columna 'city'. No puedo filtrar.")
        return
        
    df['city'] = df['city'].astype(str).str.upper().str.strip()
    
    # Obtener lista de ciudades únicas en los datos
    unique_cities = df['city'].unique()
    
    if len(unique_cities) == 0:
        print("⚠️ La tabla está vacía o no tiene ciudades definidas.")
        return

    print(f"   🏙️ Ciudades detectadas: {', '.join(unique_cities)}")
    print("="*60)

    # --- BUCLE POR CIUDAD ---
    for city in unique_cities:
        print(f"\n📍 ANALIZANDO: {city}")
        print("-" * 30)
        
        df_city = df[df['city'] == city]
        
        # Estadísticas Base (Contexto)
        avg_renta = df_city['avg_income'].mean()
        total_pop = df_city['target_pop'].sum()
        
        print(f"   • Hexágonos: {len(df_city)}")
        print(f"   • Renta Media: {avg_renta:,.0f} €")
        print(f"   • Target Pop:  {total_pop:,.0f} jóvenes")
        
        # Ejecutar tests
        issues, warnings = analyze_city(city, df_city)
        
        # Reportar
        if not issues and not warnings:
            print("   ✅ ESTADO: SALUDABLE")
        else:
            if issues:
                for i in issues: print(f"   {i}")
            if warnings:
                for w in warnings: print(f"   {w}")
                
        # Veredicto visual
        if issues:
            print(f"   🏁 VEREDICTO {city}: 🔴 REQUIERE REVISIÓN")
        elif warnings:
            print(f"   🏁 VEREDICTO {city}: 🟡 ACEPTABLE CON AVISOS")
        else:
            print(f"   🏁 VEREDICTO {city}: 🟢 LISTO PARA PROD")

    print("\n" + "="*60)
    print("FIN DEL DIAGNÓSTICO")

if __name__ == "__main__":
    run_health_check()
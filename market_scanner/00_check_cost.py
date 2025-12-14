import config
import numpy as np
import sys

def dry_run(city_name):
    print(f"\n🔮 BOLA DE CRISTAL (PRE-VUELO) PARA: {city_name}")
    print("="*60)

    if city_name not in config.CITIES:
        print(f"❌ Error: La ciudad '{city_name}' no existe en config.py")
        return

    # 1. Obtener datos de configuración
    bbox = config.CITIES[city_name]
    step = config.GRID_STEP
    categories = config.CATEGORIAS
    
    # 2. Calcular Malla (Misma lógica exacta que el script real)
    lat_steps = np.arange(bbox["min_lat"], bbox["max_lat"], step)
    lon_steps = np.arange(bbox["min_lon"], bbox["max_lon"], step)
    
    rows = len(lat_steps)
    cols = len(lon_steps)
    total_cells = rows * cols

    # 3. Estimación Financiera
    # Asumimos una media conservadora de 2.5 páginas por categoría (algunas tendrán 1, otras 6)
    AVG_PAGES = 2.5 
    COST_PER_REQUEST = 5
    
    total_requests = total_cells * len(categories) * AVG_PAGES
    total_credits = total_requests * COST_PER_REQUEST
    
    # Coste en dólares (Plan $40 = 200.000 créditos -> $0.0002/crédito)
    cost_usd = total_credits * 0.0002

    print(f"📐 GEOMETRÍA:")
    print(f"   - Bounding Box:  {bbox}")
    print(f"   - Paso de Malla: {step} grados (~1.1 km)")
    print(f"   - Dimensiones:   {rows} filas x {cols} columnas")
    print(f"   - 🕸️  TOTAL CELDAS: {total_cells}")
    print("-" * 60)
    print(f"📋 ALCANCE:")
    print(f"   - Categorías:    {len(categories)} {categories}")
    print("-" * 60)
    print(f"💰 ESTIMACIÓN DE COSTE (Aprox):")
    print(f"   - Peticiones API: ~{int(total_requests):,}")
    print(f"   - Créditos:       ~{int(total_credits):,} créditos")
    print(f"   - Coste Real:     ~${cost_usd:.2f} USD")
    print("="*60)

    if total_cells > 1000:
        print("⚠️  ALERTA: Vas a escanear más de 1.000 celdas. Asegúrate de que es lo que quieres.")
    
    # 4. LA PREGUNTA DEL MILLÓN
    response = input(f"¿Quieres proceder con el lanzamiento de {city_name}? (Escribe 'SI' para confirmar): ")
    
    if response.upper() == "SI":
        print("\n✅ Luz verde confirmada. Puedes ejecutar el script '01_fetch_city.py' ahora.")
    else:
        print("\n🛑 Operación cancelada. Revisa las coordenadas en 'config.py'.")

if __name__ == "__main__":
    # CAMBIA ESTO POR LA CIUDAD QUE QUIERAS PROBAR
    # dry_run("LASTABLAS_TEST")
    dry_run("BARCELONA")
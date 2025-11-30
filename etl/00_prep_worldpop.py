import rasterio
import numpy as np
import os
import glob

# --- CONFIGURACIÓN ---
# Carpeta donde YA tienes los ficheros .tif descargados
INPUT_DIR = "data/raw/worldpop_parts"
# Archivo final que generaremos
OUTPUT_TIF = "data/raw/target_audience_combined.tif"

def combine_rasters():
    print(f"🧪 COCINANDO DATOS DEMOGRÁFICOS DESDE: {INPUT_DIR}")
    
    # 1. BUSCAR ARCHIVOS
    # Buscamos cualquier cosa que termine en .tif dentro de la carpeta
    tif_files = glob.glob(os.path.join(INPUT_DIR, "*.tif"))
    
    if not tif_files:
        print(f"❌ ERROR: No hay archivos .tif en {INPUT_DIR}")
        print("   Por favor, mete ahí los archivos de las franjas de edad (15-19, 20-24, etc).")
        return

    print(f"   -> Encontrados {len(tif_files)} archivos para sumar.")

    # 2. INICIALIZAR ACUMULADOR
    # Necesitamos leer el primero para saber el tamaño del mapa (ancho x alto) y la georreferencia
    first_file = tif_files[0]
    meta = None
    total_array = None

    with rasterio.open(first_file) as src:
        meta = src.profile.copy()
        # Leemos el primer array para inicializar la suma
        # Reemplazamos los valores negativos (NoData de WorldPop suele ser -99999) por 0
        first_data = src.read(1)
        first_data[first_data < 0] = 0 
        total_array = first_data.astype(np.float32)

    print(f"   🔹 Base establecida con: {os.path.basename(first_file)}")

    # 3. SUMAR EL RESTO
    # Iteramos desde el segundo archivo hasta el final
    for filepath in tif_files[1:]:
        filename = os.path.basename(filepath)
        print(f"   ➕ Sumando: {filename}...")
        
        try:
            with rasterio.open(filepath) as src:
                # Verificación de seguridad: ¿Tienen el mismo tamaño?
                if src.width != meta['width'] or src.height != meta['height']:
                    print(f"      ⚠️ AVISO: {filename} tiene dimensiones distintas. Saltando...")
                    continue
                
                # Leer datos
                data = src.read(1)
                
                # Limpieza de NoData (Valores negativos a 0)
                data[data < 0] = 0
                
                # Suma matricial
                total_array += data
                
        except Exception as e:
            print(f"      ❌ Error leyendo {filename}: {e}")

    # 4. GUARDAR RESULTADO
    print(f"💾 Guardando Raster Combinado en: {OUTPUT_TIF}")
    
    # Actualizamos los metadatos para asegurar que guardamos floats
    meta.update(dtype=rasterio.float32, count=1, compress='lzw')

    with rasterio.open(OUTPUT_TIF, 'w', **meta) as dst:
        dst.write(total_array, 1)

    print("✅ ¡LISTO! Tu 'target_audience_combined.tif' está preparado.")
    print("   👉 Ahora actualiza el script '04_enrich_population.py' para que apunte a este archivo.")

if __name__ == "__main__":
    combine_rasters()
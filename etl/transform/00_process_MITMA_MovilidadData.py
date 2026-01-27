import pandas as pd
import os

# CONFIGURACIÓN
RUTA_FICHERO = '/home/jesus/spatia-li/data/raw/20250601_Viajes_distritos.csv'
DISTRITO_OBJETIVO = '2800704'
OUTPUT_PATH = '/home/jesus/spatia-li/data/raw/Alcorcon_filtrado.csv'

def robust_filter():
    if not os.path.exists(RUTA_FICHERO):
        print(f"❌ No se encuentra el archivo en: {RUTA_FICHERO}")
        return

    print("--- 🔍 Inspeccionando formato del archivo ---")
    
    # 1. Leemos solo 2 líneas para ver qué hay dentro de verdad
    with open(RUTA_FICHERO, 'r') as f:
        head = [f.readline() for _ in range(2)]
        print(f"Muestra real de los datos:\n{repr(head[0])}")

    # 2. Intentamos detectar el separador (el MITMA a veces usa | o espacios)
    sep = '\t' if '\t' in head[0] else '|' if '|' in head[0] else ','
    print(f"Separador detectado: '{repr(sep)}'")

    # 3. Leemos el fichero sin nombres de columnas primero para ver cuántas hay
    df_preview = pd.read_csv(RUTA_FICHERO, sep=sep, nrows=5, header=None)
    num_cols = len(df_preview.columns)
    print(f"El fichero tiene {num_cols} columnas.")

    # 4. Ajustamos nombres de columnas según la cantidad detectada
    if num_cols == 15:
        cols = ['fecha', 'hora', 'origen', 'destino', 'dist', 'ao', 'ad', 'frec', 'ret', 'can', 'est', 'edad', 'sexo', 'personas', 'p_km']
        col_busqueda = 'destino'
    elif num_cols == 6:
        cols = ['fecha', 'hora', 'distrito', 'residencia', 'personas', 'p_km']
        col_busqueda = 'distrito'
    else:
        cols = [f'col_{i}' for i in range(num_cols)]
        col_busqueda = cols[2] # Por defecto intentamos la tercera o cuarta

    # 5. Carga y Filtrado Flexible
    print(f"🚀 Cargando y filtrando por {DISTRITO_OBJETIVO}...")
    
    # Leemos todo convirtiendo la columna de búsqueda en string y quitando espacios
    chunks = pd.read_csv(RUTA_FICHERO, sep=sep, names=cols, dtype=str, chunksize=100000)
    
    lista_filtrada = []
    for chunk in chunks:
        # Buscamos el distrito ignorando posibles espacios en blanco
        mask = chunk[col_busqueda].str.strip() == DISTRITO_OBJETIVO
        lista_filtrada.append(chunk[mask])
    
    df_final = pd.concat(lista_filtrada)

    if df_final.empty:
        print("⚠️ El resultado sigue siendo CERO. Posibles causas:")
        print(f"   - El código {DISTRITO_OBJETIVO} no existe en este fichero.")
        print(f"   - Los códigos de distrito en el fichero son distintos (ej: solo '7908' o '28079').")
        print(f"   - Muestra de códigos encontrados en el fichero: {df_preview.iloc[:, 2 if num_cols > 2 else 0].unique()}")
    else:
        df_final['personas'] = pd.to_numeric(df_final['personas'], errors='coerce')
        print(f"✅ ¡Éxito! Encontradas {len(df_final)} filas.")
        print(f"👥 Suma total de personas: {df_final['personas'].sum():,.0f}")
        df_final.to_csv(OUTPUT_PATH, index=False)
        print(f"💾 Guardado en: {OUTPUT_PATH}")

robust_filter()
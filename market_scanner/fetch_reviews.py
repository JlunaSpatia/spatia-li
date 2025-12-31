import json
import requests
import time
import os

# --- CONFIGURACIÓN DE RUTAS Y CLAVES ---

# 1. Definimos la carpeta exacta que nos has pedido
CARPETA_BASE = os.path.join('data', 'raw', '2025_Q4')
NOMBRE_FICHERO = 'EL_PORTIL_FULL_RAW.json'
NOMBRE_SALIDA = 'EL_PORTIL_FULL_REVIEWS_COMPLETAS.json'

# Construimos las rutas completas
RUTA_FICHERO_ENTRADA = os.path.join(CARPETA_BASE, NOMBRE_FICHERO)
RUTA_FICHERO_SALIDA = os.path.join(CARPETA_BASE, NOMBRE_SALIDA)

# 2. Claves API
OLD_API_KEY = "69368e8607da3da240a81a4f"
NEW_API_KEY = "69530d7cbbe7ea6f7e8f4a03"

def obtener_todas_las_reviews(url_inicial):
    """
    Descarga todas las reseñas siguiendo la paginación y usando la NUEVA API KEY.
    """
    todas_las_reviews = []
    
    # Reemplazo inicial de la clave antigua por la nueva
    url_actual = url_inicial.replace(OLD_API_KEY, NEW_API_KEY)
    
    pagina = 1
    while url_actual:
        try:
            print(f"      -> Descargando página {pagina}...")
            response = requests.get(url_actual, timeout=30)
            
            if response.status_code != 200:
                print(f"      [!] Error {response.status_code}: {response.text}")
                break
                
            data = response.json()
            
            # Guardamos reseñas de esta página
            nuevas = data.get('reviews_results', [])
            todas_las_reviews.extend(nuevas)
            
            # Comprobar si hay siguiente página
            pagination = data.get('pagination', {})
            next_url = pagination.get('next')
            
            if next_url:
                # Lógica para inyectar SIEMPRE la clave nueva en el enlace 'next'
                if "api_key=" in next_url:
                    import re
                    # Reemplazamos cualquier api_key que venga por la nueva
                    url_actual = re.sub(r'api_key=[^&]+', f'api_key={NEW_API_KEY}', next_url)
                else:
                    # Si no trae key, la añadimos al final
                    url_actual = next_url + f"&api_key={NEW_API_KEY}"
                
                pagina += 1
                time.sleep(1) # Pausa de cortesía
            else:
                url_actual = None # Fin
                
        except Exception as e:
            print(f"      [!] Excepción durante la descarga: {e}")
            break
            
    return todas_las_reviews

def procesar_el_portil():
    print(f"--- INICIO DEL PROCESO ---")
    print(f"Buscando fichero en: {RUTA_FICHERO_ENTRADA}")
    
    if not os.path.exists(RUTA_FICHERO_ENTRADA):
        print(f"\n[ERROR CRÍTICO] No encuentro el fichero.")
        print(f"Asegúrate de estar ejecutando el script desde la raíz del proyecto.")
        print(f"Ruta actual de ejecución: {os.getcwd()}")
        return

    try:
        with open(RUTA_FICHERO_ENTRADA, 'r', encoding='utf-8') as f:
            locales = json.load(f)
    except Exception as e:
        print(f"Error al leer el JSON: {e}")
        return
    
    print(f"Se han cargado {len(locales)} locales. Empezando descarga...\n")
    
    locales_completados = []
    
    for i, local in enumerate(locales):
        titulo = local.get('title', 'Sin nombre')
        reviews_link = local.get('reviews_link')
        
        print(f"[{i+1}/{len(locales)}] Procesando: {titulo}")
        
        if reviews_link:
            reviews_totales = obtener_todas_las_reviews(reviews_link)
            
            # Guardamos los datos
            local['reviews_data'] = reviews_totales
            local['reviews_count_extracted'] = len(reviews_totales)
            
            print(f"   [OK] Total reseñas extraídas: {len(reviews_totales)}\n")
        else:
            print("   [SALTADO] No tiene reviews_link\n")
        
        locales_completados.append(local)
        
        # Guardado parcial cada 5 locales
        if (i + 1) % 5 == 0:
            with open(RUTA_FICHERO_SALIDA, 'w', encoding='utf-8') as f_out:
                json.dump(locales_completados, f_out, ensure_ascii=False, indent=4)
            print("   (Guardado parcial realizado)")

    # Guardado final
    with open(RUTA_FICHERO_SALIDA, 'w', encoding='utf-8') as f_out:
        json.dump(locales_completados, f_out, ensure_ascii=False, indent=4)
    
    print(f"\n¡TERMINADO! Fichero guardado en: {RUTA_FICHERO_SALIDA}")

if __name__ == "__main__":
    procesar_el_portil()
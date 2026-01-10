import pandas as pd
import os
import re

# CONFIGURACIÓN
INPUT_FILE = 'Categories_Google_10012025.csv'
OUTPUT_FILE = 'Categories_Full_Taxonomy.csv'

def classify_category_surgical(category_name):
    if not isinstance(category_name, str):
        return 'N', 'Other'
    
    text = category_name.lower()
    
    # --- 0. REGLAS DE ORO (Excepciones que tienen prioridad) ---
    # Si contiene esto, clasifícalo YA y no sigas comprobando.
    
    # Herbolarios y parafarmacia (Wellness)
    if 'herbolario' in text or 'parafarmacia' in text or 'plantas medicinales' in text:
        return 'Y', 'WELLNESS'
    
    # Comida a domicilio / Take away (Horeca)
    if 'domicilio' in text or 'para llevar' in text or 'preparada' in text:
        return 'Y', 'HORECA'
        
    # Barbacoas (Salvamos a la barbacoa del filtro anti-barbero)
    if 'barbacoa' in text or 'shabu' in text or 'yakiniku' in text:
        return 'Y', 'HORECA'

    # --- 1. FOOD RETAIL (Alimentación) ---
    food_keywords = [
        'supermercado', 'hipermercado', 'alimentación', 'comestibles', 
        'frutería', 'carnicería', 'pescadería', 'mercado', 'conveniencia', 
        'licorería', 'vino', 'panadería', 'bollería', 'delicatessen', 'gourmet', 
        'ultramarinos', 'congelados', 'dulces', 'golosinas', 'açaí', 'hielo',
        'pastelería', 'confitería', 'tetería', 'café', 'especias', 'abastos'
    ]
    if any(k in text for k in food_keywords): return 'Y', 'FOOD_RETAIL'

    # --- 2. HORECA (Restaurantes y Ocio) ---
    # Usamos 'restaurant' sin 'e' para pillar inglés/catalán
    horeca_keywords = [
        'restaurant', 'bar', 'cafetería', 'bistro', 'brunch', 'tostador', 
        'gastropub', 'taberna', 'cervecería', 'vinoteca', 'cocktail', 'discoteca', 
        'asador', 'pizzería', 'hamburguesería', 'sushi', 'heladería', 'churrería', 
        'tapas', 'bocatería', 'pub', 'club nocturno', 'karaoke', 'sala de conciertos',
        'catering', 'food truck', 'comida rápida', 'take away', 'chiringuito'
    ]
    
    if any(k in text for k in horeca_keywords):
        # LÓGICA DE DESEMPATE INTELIGENTE
        # Evitamos falsos positivos comunes del string "bar"
        if 'barb' in text: # Barbero, Barba...
            # Ya hemos salvado la 'barbacoa' arriba, así que esto es seguro rechazar
            pass 
        elif 'barco' in text:
            # Si es un restaurante en barco, ya entró por 'restaurant'. 
            # Si solo dice "Venta de barcos", aquí lo paramos.
            if 'restaurant' in text or 'comida' in text or 'copas' in text:
                return 'Y', 'HORECA'
            else:
                pass # Es un concesionario de barcos
        elif 'abogado' in text or 'embarazo' in text: 
            pass
        else:
            return 'Y', 'HORECA'

    # --- 3. WELLNESS (Salud y Belleza) ---
    wellness_keywords = [
        'gimnasio', 'yoga', 'pilates', 'fitness', 'crossfit', 'entrenador', 
        'deporte', 'deportivo', 'salud', 'médico', 'medicina', 'dentista', 
        'dental', 'farmacia', 'estética', 'peluquería', 'barbero', 'barbería', 
        'spa', 'masaje', 'fisioterapia', 'fisioterapeuta', 'óptica', 'podólogo', 
        'psicólogo', 'nutrición', 'hospital', 'clínica', 'bienestar', 'sauna', 
        'balneario', 'dermatolog', 'pediatra', 'boxing', 'boxeo', 'artes marciales',
        'tatuaje', 'piercing', 'uñas', 'manicura', 'depilación', 'veterinario'
    ]
    if any(k in text for k in wellness_keywords): return 'Y', 'WELLNESS'

    # --- 4. RETAIL (Tiendas) ---
    retail_keywords = [
        'tienda', 'shop', 'store', 'ropa', 'zapatería', 'moda', 'joyería', 'accesorios', 
        'electrónica', 'informática', 'muebles', 'hogar', 'regalo', 'floristería', 
        'juguetes', 'bricolaje', 'ferretería', 'mascotas', 'animales', 'papelería', 
        'perfumería', 'cosmética', 'boutique', 'outlet', 'concesionario', 'automóviles', 
        'motos', 'taller', 'reparación', 'estanco', 'tabaco', 'lotería', 'apuestas', 
        'lavandería', 'tintorería', 'gasolinera', 'estación de servicio', 'lavado',
        'fotografía', 'copistería', 'imprenta', 'telefonía', 'móviles', 'vapeo',
        'centro comercial', 'grandes almacenes', 'bazar', 'comercio', 'oro'
    ]
    # "Comerciante de oro" entrará por 'oro'. "Comercio" entrará por 'comercio'.
    if any(k in text for k in retail_keywords): return 'Y', 'RETAIL'

    # --- 5. TRAFFIC GEN ---
    traffic_keywords = [
        'colegio', 'escuela', 'universidad', 'instituto', 'guardería', 'academia',
        'oficina', 'coworking', 'hotel', 'hostal', 'apartamento', 'alojamiento',
        'cine', 'teatro', 'museo', 'parque', 'estación', 'aeropuerto', 'metro', 
        'tren', 'autobús', 'parking', 'aparcamiento', 'banco', 'cajero', 
        'correos', 'biblioteca', 'casino', 'estadio', 'arena', 'bolera', 'bingo',
        'juzgado', 'ayuntamiento', 'policía', 'bomberos', 'notaría', 'gestoría',
        'iglesia', 'templo', 'centro cultural'
    ]
    if any(k in text for k in traffic_keywords): return 'Y', 'TRAFFIC_GEN'

    return 'N', 'Other'

# --- EJECUCIÓN ---
print("🚀 Iniciando clasificación CIRUJANO v2...")

if not os.path.exists(INPUT_FILE):
    print(f"❌ Error: No encuentro '{INPUT_FILE}'")
else:
    df = pd.read_csv(INPUT_FILE)
    
    # Aplicar nueva lógica
    results = df['Category'].apply(lambda x: pd.Series(classify_category_surgical(x)))
    df['Select'] = results[0]
    df['Vertical'] = results[1]
    
    # Ordenar: Y arriba, luego por vertical
    df_sorted = df.sort_values(by=['Select', 'Vertical', 'Category'], ascending=[False, True, True])
    
    df_sorted.to_csv(OUTPUT_FILE, index=False)
    
    print("-" * 40)
    print(f"✅ HECHO. Fichero actualizado: '{OUTPUT_FILE}'")
    print(f"📊 Seleccionadas: {len(df_sorted[df_sorted['Select'] == 'Y'])}")
    print("-" * 40)
    print("🔎 Verificaciones rápidas:")
    check_list = ['barbacoa', 'herbolario', 'domicilio', 'oro']
    for check in check_list:
        found = df_sorted[
            (df_sorted['Category'].str.contains(check, case=False, na=False)) & 
            (df_sorted['Select'] == 'Y')
        ]
        print(f"   - '{check}': {len(found)} encontradas en 'Y'")
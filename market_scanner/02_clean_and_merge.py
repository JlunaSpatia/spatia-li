import json
import os
import pandas as pd

def clean_data(city_name, quarter="2025_Q4"):
    input_path = os.path.join("data", "raw", quarter, f"{city_name}_FULL_RAW.json")
    output_dir = os.path.join("data", "processed", quarter)
    output_json = os.path.join(output_dir, f"{city_name}_MASTER.json")
    output_csv = os.path.join(output_dir, f"{city_name}_MASTER.csv")
    
    os.makedirs(output_dir, exist_ok=True)
    
    if not os.path.exists(input_path):
        print(f"❌ No encuentro datos crudos en: {input_path}")
        return

    print(f"🧹 Limpiando dataset: {city_name}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    unique_places = {}
    
    for item in raw_data:
        if not isinstance(item, dict): continue
        
        # ID Único
        pid = item.get('place_id') or item.get('data_id')
        if not pid: continue
        
        # Lógica de Estado
        raw_open = str(item.get('open_state', '')).lower()
        status = "OPERATIONAL"
        if "permanently closed" in raw_open or "cerrado permanentemente" in raw_open:
            status = "CLOSED"
            
        item['standardized_status'] = status
        unique_places[pid] = item

    clean_list = list(unique_places.values())
    
    # Guardar JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(clean_list, f, ensure_ascii=False, indent=4)
        
    # Guardar CSV (Para Excel rápido)
    df = pd.DataFrame(clean_list)
    # Seleccionar columnas clave para no hacer un CSV gigante
    cols = ['title', 'type', 'reviews', 'rating', 'standardized_status', 'address', 'gps_coordinates']
    # Aplanar gps
    df['lat'] = df['gps_coordinates'].apply(lambda x: x.get('latitude') if x else None)
    df['lon'] = df['gps_coordinates'].apply(lambda x: x.get('longitude') if x else None)
    df.drop(columns=['gps_coordinates'], inplace=True, errors='ignore')
    
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    print(f"✅ Limpieza completada.")
    print(f"   Raw: {len(raw_data)} -> Únicos: {len(clean_list)}")
    print(f"   Archivos guardados en: {output_dir}")

if __name__ == "__main__":
    clean_data("LASTABLAS_TEST")
    # clean_data("VALENCIA")
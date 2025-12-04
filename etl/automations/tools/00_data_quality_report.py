import pandas as pd
import numpy as np
import os
import datetime

# --- CONFIGURACIÓN ---
FILE_LOCALES = "data/raw/locales202512.csv"
FILE_ACTIVIDAD = "data/raw/actividadeconomica202512.csv"
OUTPUT_REPORT = "data/reports/CALIDAD_DATOS_MADRID.xlsx"

# Lista para guardar los resultados
report_data = []

def add_metric(dataset, metric, value, notes=""):
    """Guarda la métrica en la lista y la imprime"""
    print(f"   [{dataset}] {metric}: {value} {notes}")
    report_data.append({
        "Fecha": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Dataset": dataset,
        "Métrica": metric,
        "Valor": value,
        "Notas": notes
    })

def clean_number_madrid(val):
    if pd.isna(val) or val == '': return 0.0
    s = str(val).strip().replace('.', '').replace(',', '.')
    try: return float(s)
    except: return 0.0

def read_robust(path):
    if not os.path.exists(path): return None
    attempts = [(';', 'utf-8-sig'), (';', 'latin-1'), (',', 'utf-8-sig'), (',', 'latin-1')]
    for sep, enc in attempts:
        try:
            df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, on_bad_lines='skip')
            if len(df.columns) > 1:
                df.columns = df.columns.str.strip().str.replace('ï»¿', '').str.replace('"', '')
                return df
        except: continue
    return None

def analyze_dataset(name, path):
    print(f"\n🔎 ANALIZANDO: {name}")
    
    df = read_robust(path)
    if df is None:
        add_metric(name, "Estado Archivo", "ERROR", "No encontrado o ilegible")
        return None

    # 1. VOLUMEN
    total_rows = len(df)
    add_metric(name, "Total Filas", f"{total_rows:,}")

    # 2. UNICIDAD ID
    col_id = next((c for c in df.columns if c.lower() == 'id_local'), None)
    if col_id:
        unique_ids = df[col_id].nunique()
        dupes = total_rows - unique_ids
        add_metric(name, "IDs Únicos", f"{unique_ids:,}")
        add_metric(name, "Duplicados", f"{dupes:,}", f"{(dupes/total_rows)*100:.1f}%")
        df.rename(columns={col_id: 'id_local'}, inplace=True)
    else:
        add_metric(name, "Columna ID", "NO ENCONTRADA")
        return None

    # 3. CALIDAD GEO
    cx_loc = next((c for c in df.columns if 'coordenada_x_local' in c.lower()), None)
    cx_agr = next((c for c in df.columns if 'coordenada_x_agrup' in c.lower()), None)

    if cx_loc:
        x_local = df[cx_loc].apply(clean_number_madrid)
        valid_local = (x_local > 1000).sum()
        add_metric(name, "Coords Individuales Válidas", f"{valid_local:,}", f"{(valid_local/total_rows)*100:.1f}%")

        if cx_agr:
            x_agrup = df[cx_agr].apply(clean_number_madrid)
            rescued = ((x_local < 1000) & (x_agrup > 1000)).sum()
            add_metric(name, "Coords Rescatadas (Agrupadas)", f"{rescued:,}", f"+{(rescued/total_rows)*100:.1f}% coverage")
            
            final_valid = valid_local + rescued
            add_metric(name, "Cobertura Geo Final", f"{final_valid:,}", f"{(final_valid/total_rows)*100:.1f}% total")

    return set(df['id_local'].unique())

def compare_datasets(ids_loc, ids_act):
    print("\n⚔️ COMPARATIVA")
    common = len(ids_loc.intersection(ids_act))
    only_loc = len(ids_loc - ids_act)
    only_act = len(ids_act - ids_loc)

    add_metric("COMPARATIVA", "Coinciden en ambos", f"{common:,}")
    add_metric("COMPARATIVA", "Solo en LOCALES", f"{only_loc:,}")
    add_metric("COMPARATIVA", "Solo en ACTIVIDAD", f"{only_act:,}")

if __name__ == "__main__":
    print("--- 📋 GENERANDO REPORTE EXCEL ---")
    
    ids_loc = analyze_dataset("LOCALES", FILE_LOCALES)
    ids_act = analyze_dataset("ACTIVIDAD", FILE_ACTIVIDAD)
    
    if ids_loc and ids_act:
        compare_datasets(ids_loc, ids_act)

    # GUARDAR EXCEL
    if not os.path.exists("data/reports"):
        os.makedirs("data/reports")
        
    df_report = pd.DataFrame(report_data)
    df_report.to_excel(OUTPUT_REPORT, index=False)
    
    print(f"\n✅ REPORTE GUARDADO EN: {OUTPUT_REPORT}")
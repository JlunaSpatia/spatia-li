import os

path_cat = "28900U_14082025.CAT" # Prueba primero con Ajalvir
path_output = "uso_inmuebles_madrid_Corregido.csv"

resultados = {}

print("🚀 Procesando con formato CAT 2006 (Posiciones 428 y 442)...")

with open(path_cat, 'r', encoding='latin-1') as f:
    for line in f:
        if line.startswith("15"):
            # Extraemos según el manual de 2022
            refcat = line[30:44].strip()   # Pos 31, long 14
            uso = line[427:428].strip()    # Pos 428, long 1
            sup_str = line[441:451].strip() # Pos 442, long 10
            
            # En el primer registro de oficina que encuentre, lo imprime para validar
            if uso == 'O' and len(resultados) < 5:
                print(f"📍 Validado: Ref {refcat} es OFICINA con {sup_str} m2")

            try:
                # El formato N suele venir multiplicado por 100 si tiene 2 decimales
                # aunque el manual dice "en metros cuadrados", CAT suele usar centiáreas
                sup = float(sup_str) / 100 
            except:
                sup = 0.0

            if refcat:
                key = (refcat, uso)
                resultados[key] = resultados.get(key, 0.0) + sup

# Guardar el CSV para ArcMap
with open(path_output, 'w') as out:
    out.write("REFCAT,USO,SUPERFICIE_TOTAL\n")
    for (rf, us), total in resultados.items():
        # Evitamos exportar si el uso quedó vacío por error
        if us:
            out.write(f"{rf},{us},{total:.2f}\n")

print(f"✅ ¡Hecho! CSV generado con {len(resultados)} registros.")
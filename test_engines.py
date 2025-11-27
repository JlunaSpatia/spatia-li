import requests
import json

# Coordenadas Valencia (Lon, Lat)
# Origen: Estación del Norte
origin = "-0.3774,39.4667"
# Destino: Torres de Serranos
dest = "-0.3760,39.4792"

def get_route(mode, port):
    url = f"http://localhost:{port}/route/v1/{mode}/{origin};{dest}?overview=false"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            duration_seconds = data['routes'][0]['duration']
            distance_meters = data['routes'][0]['distance']
            return duration_seconds, distance_meters
        else:
            return None, None
    except Exception as e:
        print(f"Error conectando a {mode}: {e}")
        return None, None

print("--- 🧪 PROBANDO MOTORES OSRM ---")

# 1. Probar PIE (Puerto 5001)
sec_walk, dist_walk = get_route("foot", 5001)
if sec_walk:
    print(f"✅ MOTOR PIE (Walk): Funciona!")
    print(f"   Tiempo andando: {sec_walk/60:.1f} min")
    print(f"   Distancia: {dist_walk:.0f} metros")
else:
    print("❌ MOTOR PIE: Falló")

# 2. Probar COCHE (Puerto 5000)
sec_drive, dist_drive = get_route("driving", 5000)
if sec_drive:
    print(f"\n✅ MOTOR COCHE (Drive): Funciona!")
    print(f"   Tiempo conduciendo: {sec_drive/60:.1f} min")
    print(f"   Distancia: {dist_drive:.0f} metros")
else:
    print("\n❌ MOTOR COCHE: Falló")

print("\n-------------------------------")
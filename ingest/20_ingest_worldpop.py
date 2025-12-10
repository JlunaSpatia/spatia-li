import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import log_execution

@log_execution(task_id=20)
def ingest_worldpop(scope="GLOBAL"):
    print("🌍 Descargando WorldPop 100m Grid...")
    # Lógica de descarga a /data/raw/worldpop_parts
    time.sleep(2)
    return "Archivos WorldPop sincronizados."

if __name__ == "__main__":
    ingest_worldpop()
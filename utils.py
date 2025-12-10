# archivo: /spatia-li/utils.py
from sqlalchemy import create_engine, text
import traceback
import functools
import inspect
import sys
import os

# Importamos config para no repetir la cadena de conexión
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config import DB_CONNECTION_STR
except ImportError:
    DB_CONNECTION_STR = "postgresql://postgres:postgres@localhost:5432/spatia"

def log_execution(task_id):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            engine = create_engine(DB_CONNECTION_STR)
            log_output = ""
            status = "RUNNING"
            
            # --- AUTO-DETECTAR SCOPE ---
            # 1. Busca argumento 'scope' o 'city' en la función
            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            vals = bound_args.arguments
            
            scope_val = vals.get('scope', vals.get('city', 'GLOBAL'))
            
            # 2. Si es global, verificamos si es una 'Release' o un 'Parche'
            # (Esto lo decidimos en el script, pero aquí aseguramos mayúsculas)
            scope_val = str(scope_val).upper()

            try:
                print(f"🚀 Iniciando Tarea {task_id} [Scope: {scope_val}]...")
                result = func(*args, **kwargs)
                
                status = "SUCCESS"
                log_output = str(result)
                print(f"✅ Tarea {task_id} terminada correctamente.")
                return result
                
            except Exception as e:
                status = "ERROR"
                log_output = f"Error: {str(e)}\n{traceback.format_exc()}"
                print(f"❌ Tarea {task_id} falló.")
                raise e
                
            finally:
                # Guardar en BBDD
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO etl_history (task_id, status, log_output, executed_by, scope) 
                            VALUES (:tid, :st, :log, 'ScriptAuto', :scp)
                        """), {"tid": task_id, "st": status, "log": log_output, "scp": scope_val})
                except Exception as db_err:
                    print(f"⚠️ Error guardando log: {db_err}")
                    
        return wrapper
    return decorator
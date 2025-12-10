import streamlit as st
import pandas as pd
import subprocess
import signal
import datetime
import os
import sys
import time
from sqlalchemy import create_engine, text

# ==========================================
# 1. SETUP DE RUTAS Y CONFIGURACIÓN
# ==========================================
# Tu archivo está en: .../spatia-li/app/pages/admin_ops.py

current_dir = os.path.dirname(os.path.abspath(__file__)) # .../pages
app_dir = os.path.dirname(current_dir)                   # .../app
project_root = os.path.dirname(app_dir)                  # .../spatia-li (RAÍZ REAL)

# Añadimos la raíz al sistema para poder importar config y utils
sys.path.append(project_root)

# Importamos la configuración centralizada
try:
    from config import DB_CONNECTION_STR, ACTIVE_CITIES
except ImportError:
    st.error(f"❌ Error Crítico: No encuentro 'config.py'.\nBuscando en: {project_root}")
    st.stop()

# Configuración visual de Streamlit
st.set_page_config(page_title="Ops Control Center", page_icon="🎛️", layout="wide")

# Inicializar estado para procesos en segundo plano
if 'running_tasks' not in st.session_state:
    st.session_state.running_tasks = {}

# Estilos CSS
st.markdown("""
<style>
    .status-box { padding: 8px; border-radius: 5px; text-align: center; font-weight: bold; font-size: 0.9em;}
    .due { background-color: #ffebe9; color: #cf222e; border: 1px solid #cf222e; }
    .done { background-color: #dafbe1; color: #1a7f37; border: 1px solid #1a7f37; }
    .running { background-color: #e0f2fe; color: #0284c7; border: 1px solid #0284c7; animation: pulse 2s infinite;}
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
    div.stButton > button {width: 100%;}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. LÓGICA DE BACKEND
# ==========================================
def get_engine():
    return create_engine(DB_CONNECTION_STR)

def get_smart_status():
    """
    Construye la tabla de tareas combinando:
    - Tareas Globales (1 fila única)
    - Tareas Multi-Ciudad (1 fila por cada ciudad en ACTIVE_CITIES)
    """
    engine = get_engine()
    # Leemos la definición de tareas
    definitions = pd.read_sql("SELECT * FROM etl_definitions ORDER BY task_id", engine)
    
    rows = []
    
    for _, task in definitions.iterrows():
        tid = task['task_id']
        
        # --- LÓGICA: ¿Es una tarea por ciudad? (Ej: ID 30) ---
        MULTI_CITY_TASKS = [30] 
        
        if tid in MULTI_CITY_TASKS:
            for city in ACTIVE_CITIES:
                sql = text("""
                    SELECT MAX(run_date) FROM etl_history 
                    WHERE task_id = :tid AND scope = :city AND status = 'SUCCESS'
                """)
                with engine.connect() as conn:
                    last_run = conn.execute(sql, {"tid": tid, "city": city}).scalar()
                
                rows.append({
                    "unique_id": f"{tid}_{city}",
                    "task_id": tid,
                    "display_name": f"{task['task_name']} ({city})",
                    "scope": city,
                    "desc": task['description'],
                    "freq": task['frequency_days'],
                    "last_run": last_run,
                    "script_path": task['script_path']
                })

        else:
            # --- Tareas Globales ---
            sql = text("""
                SELECT MAX(run_date) FROM etl_history 
                WHERE task_id = :tid 
                AND (scope = 'GLOBAL' OR scope = 'GLOBAL_RELEASE') 
                AND status = 'SUCCESS'
            """)
            with engine.connect() as conn:
                last_run = conn.execute(sql, {"tid": tid}).scalar()
            
            rows.append({
                "unique_id": f"{tid}_GLB",
                "task_id": tid,
                "display_name": task['task_name'],
                "scope": "GLOBAL_RELEASE", 
                "desc": task['description'],
                "freq": task['frequency_days'],
                "last_run": last_run,
                "script_path": task['script_path']
            })
            
    return pd.DataFrame(rows)

def start_task(unique_id, task_id, script_rel_path, scope_arg):
    """Lanza el script en segundo plano sin bloquear la App"""
    
    full_path = os.path.join(project_root, script_rel_path)
    
    if not os.path.exists(full_path):
        st.toast(f"❌ Error: No encuentro el script en {full_path}", icon="🔥")
        return

    cmd = ["python", full_path]
    if scope_arg:
        cmd.append(scope_arg)

    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
        
        proc = subprocess.Popen(
            cmd, 
            cwd=project_root,
            env=env,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True,
            preexec_fn=os.setsid
        )
        st.session_state.running_tasks[unique_id] = {'proc': proc, 'start': datetime.datetime.now()}
        st.rerun()
        
    except Exception as e:
        st.error(f"Fallo al arrancar el proceso: {e}")

def stop_task(unique_id):
    if unique_id in st.session_state.running_tasks:
        proc = st.session_state.running_tasks[unique_id]['proc']
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except: pass
        del st.session_state.running_tasks[unique_id]
        st.rerun()

def check_tasks_status():
    finished_ids = []
    for uid, info in st.session_state.running_tasks.items():
        if info['proc'].poll() is not None:
            finished_ids.append(uid)
            if info['proc'].returncode == 0:
                st.toast(f"Tarea completada: {uid}", icon="✅")
            else:
                st.toast(f"Tarea falló: {uid}", icon="❌")
    
    if finished_ids:
        for uid in finished_ids:
            del st.session_state.running_tasks[uid]
        st.rerun()

# ==========================================
# 3. INTERFAZ DE USUARIO (UI)
# ==========================================
st.title("🎛️ Ops Center")
st.caption(f"Proyecto Spatia | Entorno: {project_root}")
st.divider()

check_tasks_status()

# Cargar tabla inteligente
try:
    df_tasks = get_smart_status()
except Exception as e:
    st.error(f"Error conectando a BBDD: {e}")
    st.stop()

for _, row in df_tasks.iterrows():
    uid = row['unique_id']
    is_running = uid in st.session_state.running_tasks
    
    # Semáforo
    last = row['last_run']
    if is_running:
        status_html = '<div class="status-box running">⚙️ RUNNING</div>'
    elif pd.isna(last):
        status_html = '<div class="status-box never">⚪ NUNCA</div>'
        is_due = True
    else:
        days = (datetime.datetime.now() - last).days
        if days >= row['freq']:
            status_html = f'<div class="status-box due">🔴 PENDIENTE ({days}d)</div>'
            is_due = True
        else:
            status_html = f'<div class="status-box done">🟢 AL DÍA ({days}d)</div>'
            is_due = False

    with st.container():
        c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1.5])
        
        c1.markdown(f"**{row['display_name']}**")
        c1.caption(f"📝 {row['desc']}")
        
        c2.info(f"📅 Frec: {row['freq']} días")
        c3.markdown(status_html, unsafe_allow_html=True)
        
        with c4:
            st.write("")
            if is_running:
                st.button("⛔ STOP", key=f"stop_{uid}", type="primary", on_click=stop_task, args=(uid,))
            else:
                label = "▶️ RUN" if is_due else "🔄 RE-RUN"
                st.button(label, key=f"run_{uid}", on_click=start_task, 
                          args=(uid, row['task_id'], row['script_path'], row['scope']))
    st.divider()

if st.session_state.running_tasks:
    time.sleep(2)
    st.rerun()
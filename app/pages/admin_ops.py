import streamlit as st
import pandas as pd
import subprocess
from sqlalchemy import create_engine, text
import datetime
import os

# --- CONFIG ---
st.set_page_config(page_title="Ops Control Center", page_icon="🎛️", layout="wide")
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

# CSS para estados
st.markdown("""
<style>
    .status-box {padding: 10px; border-radius: 5px; margin-bottom: 10px; font-weight: bold;}
    .due {background-color: #ffcccb; color: #8b0000; border: 1px solid #8b0000;}
    .done {background-color: #d4edda; color: #155724; border: 1px solid #155724;}
    .btn-run {width: 100%;}
</style>
""", unsafe_allow_html=True)

def get_engine():
    return create_engine(DB_URL)

def get_tasks_status():
    """Calcula qué tareas tocan hoy basándose en la última ejecución"""
    engine = get_engine()
    
    # Query maestra: Une Definiciones con la Última Ejecución Exitosa
    sql = """
    SELECT 
        d.task_id, d.task_name, d.script_path, d.frequency_days, d.description,
        MAX(h.run_date) as last_run
    FROM etl_definitions d
    LEFT JOIN etl_history h ON d.task_id = h.task_id AND h.status = 'SUCCESS'
    GROUP BY d.task_id, d.task_name, d.script_path, d.frequency_days, d.description
    ORDER BY d.task_id ASC;
    """
    return pd.read_sql(sql, engine)

def run_script(task_id, script_path):
    """Ejecuta el script python y guarda el log"""
    engine = get_engine()
    
    # Verificar existencia
    if not os.path.exists(script_path):
        return False, f"❌ Archivo no encontrado: {script_path}"
    
    try:
        # Ejecución real
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            check=True
        )
        status = "SUCCESS"
        log = result.stdout
        is_ok = True
        
    except subprocess.CalledProcessError as e:
        status = "ERROR"
        log = e.stdout + "\n" + e.stderr
        is_ok = False
        
    # Guardar en Historial
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO etl_history (task_id, status, log_output) 
            VALUES (:tid, :st, :log)
        """), {"tid": task_id, "st": status, "log": log})
        conn.commit()
        
    return is_ok, log

# --- INTERFAZ PRINCIPAL ---
st.title("🎛️ Centro de Operaciones ETL")
st.markdown("Gestión manual de procesos recurrentes y mantenimiento de datos.")

df_tasks = get_tasks_status()

# Iteramos por cada tarea para pintar su tarjeta
for index, row in df_tasks.iterrows():
    with st.container():
        # Cálculo de estado
        last_run = row['last_run']
        freq = row['frequency_days']
        
        if pd.isna(last_run):
            days_ago = 999
            status_text = "NUNCA EJECUTADO"
            is_due = True
        else:
            days_ago = (datetime.datetime.now() - last_run).days
            status_text = f"Hace {days_ago} días ({last_run.strftime('%Y-%m-%d')})"
            is_due = days_ago >= freq

        # Diseño de la Tarjeta (Columnas)
        c1, c2, c3, c4, c5 = st.columns([0.5, 2, 1.5, 1.5, 1])
        
        # 1. ID
        c1.markdown(f"**#{row['task_id']}**")
        
        # 2. Nombre y Descripción
        c2.markdown(f"**{row['task_name']}**")
        c2.caption(row['description'])
        
        # 3. Frecuencia
        c3.text(f"📅 Cada {freq} días")
        
        # 4. Estado (Visual)
        if is_due:
            c4.markdown(f'<div class="status-box due">🔴 PENDIENTE<br><small>{status_text}</small></div>', unsafe_allow_html=True)
        else:
            c4.markdown(f'<div class="status-box done">🟢 AL DÍA<br><small>{status_text}</small></div>', unsafe_allow_html=True)
            
        # 5. BOTÓN DE ACCIÓN (RUN)
        if c5.button("▶️ EJECUTAR", key=f"btn_{row['task_id']}", type="primary" if is_due else "secondary"):
            
            with st.status(f"Ejecutando: {row['task_name']}...", expanded=True) as status:
                st.write("🚀 Iniciando proceso...")
                st.code(f"python {row['script_path']}", language="bash")
                
                success, log = run_script(row['task_id'], row['script_path'])
                
                if success:
                    status.update(label="✅ Completado con éxito!", state="complete", expanded=False)
                    st.success("Proceso finalizado correctamente.")
                    st.rerun() # Recargar para actualizar fechas
                else:
                    status.update(label="❌ Error en la ejecución", state="error")
                    st.error("El script ha fallado. Revisa el log abajo.")
                    st.text_area("Log de Error", log, height=200)

    st.divider()

# --- SECCIÓN DE HISTORIAL ---
with st.expander("📜 Ver Historial Completo de Ejecuciones"):
    engine = get_engine()
    history = pd.read_sql("""
        SELECT h.run_date, d.task_name, h.status, LEFT(h.log_output, 100) as log_preview
        FROM etl_history h
        JOIN etl_definitions d ON h.task_id = d.task_id
        ORDER BY h.run_date DESC LIMIT 50
    """, engine)
    st.dataframe(history, use_container_width=True)
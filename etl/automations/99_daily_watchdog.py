import pandas as pd
from sqlalchemy import create_engine
import requests
import datetime
import os

# --- CONFIGURACIÓN ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"
DASHBOARD_URL = "http://localhost:8501/admin_ops"

# TUS CREDENCIALES DE TELEGRAM
TELEGRAM_TOKEN = "8206652906:AAFyeetJLGBiOsbVMi0eCbw0bCKoKUPJKS4"
TELEGRAM_CHAT_ID = "7017713378"

def send_telegram_alert(message):
    """Envía mensaje al móvil"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"Error enviando alerta: {e}")

def check_system_health():
    print("🐕 WATCHDOG: Revisando estado del sistema...")
    engine = create_engine(DB_URL)
    
    # 1. CONSULTA INTELIGENTE
    # Busca tareas cuya última ejecución exitosa fue hace más días de los permitidos
    sql = """
    WITH last_runs AS (
        SELECT task_id, MAX(run_date) as last_success
        FROM etl_history 
        WHERE status = 'SUCCESS'
        GROUP BY task_id
    )
    SELECT 
        d.task_name, 
        d.frequency_days,
        COALESCE(l.last_success, '2000-01-01') as last_run,
        EXTRACT(DAY FROM (NOW() - COALESCE(l.last_success, '2000-01-01'))) as days_ago
    FROM etl_definitions d
    LEFT JOIN last_runs l ON d.task_id = l.task_id
    WHERE 
        EXTRACT(DAY FROM (NOW() - COALESCE(l.last_success, '2000-01-01'))) >= d.frequency_days
    """
    
    df_overdue = pd.read_sql(sql, engine)
    
    # 2. EVALUAR Y AVISAR
    if not df_overdue.empty:
        print(f"   ⚠️ Se han detectado {len(df_overdue)} tareas pendientes.")
        
        msg_lines = ["🚨 *ALERTA RETAIL GENOME* 🚨", ""]
        msg_lines.append(f"Hay *{len(df_overdue)} procesos* que requieren tu atención hoy:")
        msg_lines.append("")
        
        for _, row in df_overdue.iterrows():
            dias = int(row['days_ago'])
            # Icono según gravedad
            icon = "🟠" if dias < row['frequency_days'] + 5 else "🔴"
            msg_lines.append(f"{icon} *{row['task_name']}*")
            msg_lines.append(f"   └─ Pendiente hace: {dias} días (Frecuencia: {row['frequency_days']}d)")
        
        msg_lines.append("")
        msg_lines.append(f"👉 [Abrir Panel de Control]({DASHBOARD_URL})")
        
        final_msg = "\n".join(msg_lines)
        send_telegram_alert(final_msg)
        print("   ✅ Alerta enviada a Telegram.")
        
    else:
        print("   ✅ Todo el sistema está al día. No molestar al jefe.")

if __name__ == "__main__":
    # Solo ejecuta si hay token configurado para no fallar en test
    if "PEGAR_TU" not in TELEGRAM_TOKEN:
        check_system_health()
    else:
        print("❌ Configura tu Token de Telegram en el script primero.")
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.graph_objects as go
from sqlalchemy import create_engine
from openai import OpenAI

# ==========================================
# 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS
# ==========================================
st.set_page_config(
    layout="wide", 
    page_title="Retail Genome | Blue Banana",
    page_icon="🍌",
    initial_sidebar_state="expanded"
)

# Inyección de CSS para Look & Feel Profesional
st.markdown("""
<style>
    /* Fuente General */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Ajustes de Contenedor */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
    }

    /* Títulos */
    h1, h2, h3 {
        font-weight: 800 !important;
        color: #FFFFFF !important;
    }

    /* Métricas (KPIs) */
    div[data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        color: #F3C623 !important; /* Amarillo Corporativo */
        font-weight: 600;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
        color: #a0a0a0 !important;
    }

    /* Mapa PyDeck */
    div[data-testid="stDeckGlJsonChart"] {
        height: 650px !important; /* Altura fija para alineación */
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    }

    /* Panel Derecho (Tarjeta de Detalle) */
    .detail-card {
        background-color: #1A1C24;
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #2C2F3F;
        box-shadow: inset 0 0 20px rgba(0,0,0,0.2);
        height: 650px; /* Misma altura que el mapa */
        overflow-y: auto; /* Scroll si el contenido es largo */
    }
    
    /* Caja de Respuesta IA */
    .ai-box {
        background-color: #232530;
        border-left: 4px solid #F3C623;
        padding: 15px;
        margin-top: 15px;
        border-radius: 6px;
        font-size: 0.9rem;
        line-height: 1.5;
        color: #E0E0E0;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. GESTIÓN DE ESTADO (MEMORIA DE SESIÓN)
# ==========================================
# Esto evita que la selección o la IA desaparezcan al recargar
if 'selected_hex' not in st.session_state:
    st.session_state.selected_hex = None

if 'ai_insight' not in st.session_state:
    st.session_state.ai_insight = None

# ==========================================
# 3. CONEXIÓN Y CARGA DE DATOS
# ==========================================
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

@st.cache_data
def load_data(city):
    try:
        engine = create_engine(DB_URL)
        # Traemos todos los datos necesarios
        return pd.read_sql(f"SELECT * FROM retail_results WHERE city = '{city}'", engine)
    except Exception as e:
        st.error(f"Error conectando a PostGIS: {e}")
        return pd.DataFrame()

# ==========================================
# 4. FUNCIONES AUXILIARES (GRÁFICOS & IA)
# ==========================================
def make_radar_chart(row):
    """Crea el gráfico de araña comparativo"""
    categories = ['Afinidad', 'Cercanía Café', 'Cercanía Gym', 'Sinergia Retail']
    # Normalizamos valores a escala 0-100 para visualización
    values = [
        row['similarity'],
        row['dist_cafe_score'] * 100,
        row['dist_gym_score'] * 100,
        row['dist_shop_score'] * 100
    ]
    
    fig = go.Figure()
    # Capa Candidato
    fig.add_trace(go.Scatterpolar(
        r=values, theta=categories, fill='toself', name='Ubicación',
        line_color='#F3C623', fillcolor='rgba(243, 198, 35, 0.3)'
    ))
    # Capa Ideal
    fig.add_trace(go.Scatterpolar(
        r=[100]*4, theta=categories, name='Ideal',
        line_color='rgba(255, 255, 255, 0.3)', line_dash='dot', hoverinfo='skip'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], color='grey', showticklabels=False),
            angularaxis=dict(color='white'),
            bgcolor='rgba(0,0,0,0)'
        ),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=30, r=30, t=30, b=30),
        height=250,
        font=dict(family="Inter")
    )
    return fig

def generate_ai_insight(api_key, row):
    """Consulta a OpenAI"""
    if not api_key: 
        return "⚠️ Error: Falta la API Key de OpenAI en la barra lateral."
    
    client = OpenAI(api_key=api_key)
    prompt = f"""
    Eres un Consultor Senior de Retail Location para 'Blue Banana' (Target: Gen Z, Aventura, Nivel medio-alto).
    
    Analiza esta ubicación en {row['city']}:
    - Score Global: {row['similarity']:.1f}/100.
    - Café Especialidad: a {row['dist_cafe']:.0f} seg.
    - Crossfit/Gym: a {row['dist_gym']:.0f} seg.
    - Competencia: a {row['dist_shop']:.0f} seg.
    
    Escribe un "Investment Memo" de 3 puntos:
    1. Veredicto (Go/No-Go).
    2. Por qué encaja (o no) con el estilo de vida de la marca.
    3. Una recomendación táctica.
    """
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        return res.choices[0].message.content
    except Exception as e: 
        return f"❌ Error conectando con la IA: {e}"

# ==========================================
# 5. BARRA LATERAL (CONTROLES)
# ==========================================
with st.sidebar:
    st.title("🍌 Retail Genome")
    st.caption("Site Selection Intelligence v1.0")
    st.divider()
    
    # Selectores
    city = st.selectbox("📍 Ciudad", ["Valencia", "Madrid"])
    
    st.markdown("### 🎯 Estrategia")
    # Slider de Rango (Min - Max)
    score_range = st.slider("Rango de Afinidad (%)", 0.0, 100.0, (75.0, 100.0))
    # Filtro manual
    max_cafe = st.number_input("Máx min. a Café", value=10) * 60 # a segundos

    st.divider()
    
    st.markdown("### 🧠 Inteligencia")
    openai_key = st.text_input("OpenAI API Key", type="password", placeholder="sk-...")

# ==========================================
# 6. PROCESAMIENTO DE DATOS
# ==========================================
df = load_data(city)

if df.empty:
    st.warning("⚠️ No hay datos cargados. Ejecuta los scripts ETL primero.")
    st.stop()

# Filtros Pandas
mask = (
    (df['similarity'] >= score_range[0]) & 
    (df['similarity'] <= score_range[1]) & 
    (df['dist_cafe'] <= max_cafe)
)
df_filtered = df[mask].copy()

# Asignación de colores semánticos para el mapa
def get_hex_color(score):
    if score >= 92: return [243, 198, 35, 255]   # Amarillo (Top)
    if score >= 85: return [64, 224, 208, 240]   # Turquesa
    if score >= 75: return [30, 144, 255, 220]   # Azul
    return [100, 110, 120, 180]                  # Gris (Bajo)

df_filtered['color'] = df_filtered['similarity'].apply(get_hex_color)

# ==========================================
# 7. LAYOUT PRINCIPAL
# ==========================================

# Dos columnas: Mapa (ancho) y Detalles (estrecho)
col_map, col_details = st.columns([2.3, 1])

# --- COLUMNA IZQUIERDA: MAPA ---
with col_map:
    # Fila de KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Oportunidades", f"{len(df_filtered)}")
    k2.metric("Top Afinidad", f"{df_filtered['similarity'].max():.1f}%" if not df_filtered.empty else "0%")
    k3.metric("Mejor Acceso Café", f"{df_filtered['dist_cafe'].min()/60:.1f} min" if not df_filtered.empty else "-")

    # Configuración de Vista Inicial
    if not df_filtered.empty:
        lat, lon, zoom = df_filtered['lat'].mean(), df_filtered['lon'].mean(), 12.5
    else:
        lat, lon, zoom = df['lat'].mean(), df['lon'].mean(), 11

    # Definición del Mapa (PyDeck)
    deck = pdk.Deck(
        layers=[pdk.Layer(
            "H3HexagonLayer",
            df_filtered,
            pickable=True,
            stroked=True,
            filled=True,
            extruded=True,
            get_hexagon="h3_index",
            get_fill_color="color",
            get_elevation="similarity",
            elevation_scale=12,
            elevation_range=[0, 800],
            coverage=0.85,
            auto_highlight=True,
        )],
        initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom, pitch=50, bearing=-10),
        map_style=pdk.map_styles.CARTO_DARK, # Estilo Oscuro Premium
        tooltip={"html": "<b>Afinidad: {similarity:.1f}%</b><br>Click para analizar"}
    )
    
    # RENDERIZADO CON EVENTO DE SELECCIÓN
    # on_select="rerun" es clave para la interactividad
    event = st.pydeck_chart(deck, on_select="rerun", selection_mode="single-object", use_container_width=True)

# --- LÓGICA DE SELECCIÓN ---
# 1. ¿El usuario hizo clic en el mapa?
if event.selection and len(event.selection['objects']) > 0:
    new_hex = event.selection['objects'][0]['h3_index']
    # Si cambió de hexágono, reseteamos el análisis de IA
    if new_hex != st.session_state.selected_hex:
        st.session_state.ai_insight = None
    st.session_state.selected_hex = new_hex

# 2. Recuperamos los datos del hexágono seleccionado (desde la memoria)
selected_row = None
if st.session_state.selected_hex:
    # Buscamos en el dataframe completo (no solo el filtrado, por si acaso)
    rows = df[df['h3_index'] == st.session_state.selected_hex]
    if not rows.empty:
        selected_row = rows.iloc[0]

# --- COLUMNA DERECHA: FICHA DE DETALLE ---
with col_details:
    with st.container():
        # Abrimos el contenedor con estilo CSS personalizado
        st.markdown('<div class="detail-card">', unsafe_allow_html=True)
        
        if selected_row is not None:
            # CABECERA
            st.subheader("📍 Análisis de Zona")
            st.caption(f"Hexágono ID: {selected_row['h3_index']}")
            
            # GRÁFICO RADAR
            st.plotly_chart(make_radar_chart(selected_row), use_container_width=True, config={'displayModeBar': False})
            
            # MÉTRICAS CLAVE
            c1, c2 = st.columns(2)
            c1.metric("Score Total", f"{selected_row['similarity']:.1f}%")
            c2.metric("Café (Pie)", f"{selected_row['dist_cafe']/60:.1f} min")
            
            st.divider()
            
            # SECCIÓN INTELIGENCIA ARTIFICIAL
            st.subheader("🤖 AI Investment Memo")
            
            # Botón (Solo si no hay insight generado ya)
            if st.session_state.ai_insight is None:
                if st.button("Generar Informe Ejecutivo", type="primary", use_container_width=True):
                    with st.spinner("El analista virtual está estudiando la zona..."):
                        insight = generate_ai_insight(openai_key, selected_row)
                        st.session_state.ai_insight = insight
                        st.rerun() # Recargamos para mostrar el resultado
            
            # Mostrar Resultado (Persistente)
            if st.session_state.ai_insight:
                st.markdown(f'<div class="ai-box">{st.session_state.ai_insight}</div>', unsafe_allow_html=True)
                
                if st.button("🔄 Nuevo Análisis", help="Borrar y generar de nuevo"):
                    st.session_state.ai_insight = None
                    st.rerun()
                    
        else:
            # ESTADO VACÍO (INSTRUCCIONES)
            st.info("👈 Selecciona una columna en el mapa para ver su ficha técnica.")
            
            st.markdown("---")
            st.markdown("**🏆 Top 5 Candidatos Globales:**")
            st.dataframe(
                df_filtered.sort_values('similarity', ascending=False).head(5)[['h3_index', 'similarity']],
                hide_index=True,
                use_container_width=True
            )

        st.markdown('</div>', unsafe_allow_html=True) # Cierre del div detail-card
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine
from openai import OpenAI

# ==========================================
# 1. CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(
    layout="wide", 
    page_title="Retail Genome | Top Picks",
    page_icon="🏆",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] {font-family: 'Inter', sans-serif;}
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    
    div[data-testid="stDeckGlJsonChart"] {
        height: 600px !important;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    }
    .detail-card {
        background-color: #1A1C24;
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #2C2F3F;
        height: 1000px;
        overflow-y: auto;
    }
    .ai-box {
        background-color: #232530;
        border-left: 4px solid #F3C623;
        padding: 15px;
        border-radius: 6px;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. ESTADO
# ==========================================
if 'selected_hex' not in st.session_state: st.session_state.selected_hex = None
if 'ai_insight' not in st.session_state: st.session_state.ai_insight = None

# ==========================================
# 3. DATOS
# ==========================================
DB_URL = "postgresql://postgres:postgres@localhost:5432/spatia"

@st.cache_data
def load_data(city):
    try:
        engine = create_engine(DB_URL)
        return pd.read_sql(f"SELECT * FROM retail_results WHERE city = '{city}'", engine)
    except Exception as e:
        st.error(f"Error BD: {e}")
        return pd.DataFrame()

# ==========================================
# 4. GRÁFICOS
# ==========================================
def make_radar_chart(row):
    categories = ['Afinidad', 'Cercanía Café', 'Cercanía Gym', 'Sinergia Retail']
    values = [
        row['similarity'],
        row.get('dist_cafe_score', 0) * 100,
        row.get('dist_gym_score', 0) * 100,
        row.get('dist_shop_score', 0) * 100
    ]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=values, theta=categories, fill='toself', name='Candidato', line_color='#F3C623'))
    fig.add_trace(go.Scatterpolar(r=[100]*4, theta=categories, name='Ideal', line_color='rgba(255,255,255,0.3)', line_dash='dot'))
    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100], color='grey'), bgcolor='rgba(0,0,0,0)'), 
                      showlegend=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                      margin=dict(l=30, r=30, t=30, b=30), height=250, font=dict(family="Inter"))
    return fig

def make_opportunity_scatter(data):
    # El gráfico se adapta a los Top N seleccionados
    avg_rent = data['est_monthly_rent'].mean() if 'est_monthly_rent' in data.columns else 2000
    
    def classify(row):
        rent = row.get('est_monthly_rent', 9999)
        score = row['similarity']
        # Lógica simplificada para Top N
        if score >= 90 and rent < avg_rent: return '💎 GEMA'
        return '⭐ CANDIDATO'

    data['Tipo'] = data.apply(classify, axis=1)
    
    fig = px.scatter(
        data, x="est_monthly_rent", y="similarity", color="Tipo", size="similarity",
        hover_data=["district_name", "ranking"], # Mostramos el ranking en el tooltip
        color_discrete_map={"💎 GEMA": "#00FF00", "⭐ CANDIDATO": "#FFD700"},
        title=f"ROI: Top {len(data)} Candidatos",
        labels={"est_monthly_rent": "Alquiler (€/mes)", "similarity": "Score IA"}
    )
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
    return fig

def generate_ai_insight(api_key, row):
    if not api_key: return "⚠️ Falta API Key."
    client = OpenAI(api_key=api_key)
    
    prompt = f"""
    Eres Director de Expansión. Estás evaluando la opción #{row.get('ranking', '?')} del ranking.
    Ubicación: {row['city']} ({row.get('district_name', 'Zona')}).
    Datos:
    - Score IA: {row['similarity']:.1f}/100.
    - Alquiler: {row.get('est_monthly_rent',0):.0f}€ ({row.get('price_m2',0):.0f}€/m2).
    - Entorno: Café a {row['dist_cafe']:.0f}s, Gym a {row['dist_gym']:.0f}s.
    
    Redacta un veredicto directo de 3 líneas: ¿Por qué esta opción está en el Top? ¿Qué riesgo tiene?
    """
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}])
        return res.choices[0].message.content
    except Exception as e: return f"Error IA: {e}"

# ==========================================
# 5. SIDEBAR (NUEVA LÓGICA DE FILTRADO)
# ==========================================
with st.sidebar:
    st.title("🏆 Top Picks")
    city = st.selectbox("📍 Ciudad", ["Valencia", "Madrid"])
    
    st.divider()
    
    # 1. FILTRO DE CANTIDAD (EL RANKING)
    st.markdown("### 🥇 Ranking")
    top_n = st.slider("Mostrar Top N Resultados", 1, 50, 10)
    
    # 2. FILTROS DUROS (CONSTRAINT)
    st.markdown("### 🚫 Líneas Rojas (Constraints)")
    max_rent = st.number_input("Presupuesto Máx (€/mes)", value=6000, step=500)
    max_dist_metro = st.slider("Máx. min a Metro", 5, 30, 15) * 60 # a segundos
    
    st.divider()
    
    st.markdown("### 🎨 Capas")
    layer_mode = st.radio("Color Mapa:", ["Ranking (Oro=Top 1)", "Precio (€)"], index=0)
    
    st.divider()
    openai_key = st.text_input("OpenAI Key", type="password")

# ==========================================
# 6. LÓGICA DE NEGOCIO (SORT & SLICE)
# ==========================================
df = load_data(city)
if df.empty: st.stop()

# 1. Aplicamos restricciones duras (Budget y Metro)
mask_hard = (
    (df.get('est_monthly_rent', 0) <= max_rent) & 
    (df['dist_transit'] <= max_dist_metro)
)
df_valid = df[mask_hard].copy()

# 2. ORDENAMOS por Score (De mejor a peor)
df_sorted = df_valid.sort_values('similarity', ascending=False).reset_index(drop=True)

# 3. CORTAMOS (Top N)
df_top = df_sorted.head(top_n).copy()

# 4. Añadimos columna de Ranking (1, 2, 3...)
df_top['ranking'] = df_top.index + 1

# Colores Dinámicos para el Mapa
def get_color(row, mode):
    if mode == "Precio (€)":
        price = row.get('est_monthly_rent', 0)
        if price > 4000: return [255, 75, 75, 200] # Rojo
        return [75, 255, 75, 200] # Verde
    else: # Por Ranking
        rank = row['ranking']
        if rank == 1: return [255, 215, 0, 255]   # Oro (Top 1)
        if rank <= 3: return [192, 192, 192, 240] # Plata (Top 3)
        if rank <= 10: return [205, 127, 50, 220] # Bronce
        return [100, 100, 255, 180]               # Azul (Resto)

df_top['color'] = df_top.apply(lambda x: get_color(x, layer_mode), axis=1)

# ==========================================
# 7. LAYOUT
# ==========================================
col_map, col_details = st.columns([2.3, 1])

with col_map:
    # KPI Row
    k1, k2, k3 = st.columns(3)
    k1.metric("Candidatos Filtrados", f"{len(df_top)}")
    
    # Datos del Ganador (Rank 1)
    if not df_top.empty:
        winner = df_top.iloc[0]
        k2.metric("🏆 Ganador (Score)", f"{winner['similarity']:.1f}")
        k3.metric("Alquiler Ganador", f"{winner.get('est_monthly_rent',0):.0f}€")
    
    # Mapa
    lat, lon = (df_top['lat'].mean(), df_top['lon'].mean()) if not df_top.empty else (40.4, -3.7)
    
    deck = pdk.Deck(
        layers=[pdk.Layer(
            "H3HexagonLayer", df_top, pickable=True, extruded=True,
            get_hexagon="h3_index", get_fill_color="color", get_elevation="similarity",
            elevation_scale=15, elevation_range=[0, 800], coverage=0.85, auto_highlight=True
        )],
        initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=12.5, pitch=50),
        map_style=pdk.map_styles.CARTO_DARK,
        tooltip={"html": "<b>RANKING: #{ranking}</b><br>Score: {similarity:.1f}<br>Alquiler: {est_monthly_rent:.0f}€"}
    )
    event = st.pydeck_chart(deck, on_select="rerun", selection_mode="single-object", use_container_width=True)
    
    # Gráfico Scatter (Solo de los Top N)
    st.divider()
    if not df_top.empty and 'est_monthly_rent' in df_top.columns:
        st.plotly_chart(make_opportunity_scatter(df_top), use_container_width=True)

# Lógica Selección
if event.selection and len(event.selection['objects']) > 0:
    st.session_state.selected_hex = event.selection['objects'][0]['h3_index']
    if st.session_state.selected_hex != event.selection['objects'][0]['h3_index']: st.session_state.ai_insight = None

selected_row = None
if st.session_state.selected_hex:
    rows = df_top[df_top['h3_index'] == st.session_state.selected_hex] # Buscamos solo en los top
    if not rows.empty: selected_row = rows.iloc[0]

with col_details:
    with st.container():
        st.markdown('<div class="detail-card">', unsafe_allow_html=True)
        if selected_row is not None:
            # CABECERA DESTACADA
            st.markdown(f"### 🎖️ Opción #{selected_row['ranking']}")
            st.caption(f"ID: {selected_row['h3_index']} | {selected_row.get('district_name', '')}")
            
            st.plotly_chart(make_radar_chart(selected_row), use_container_width=True, config={'displayModeBar': False})
            
            c1, c2 = st.columns(2)
            c1.metric("Score IA", f"{selected_row['similarity']:.1f}")
            c2.metric("Alquiler", f"{selected_row.get('est_monthly_rent',0):.0f}€")
            
            st.divider()
            if st.session_state.ai_insight is None:
                if st.button("🤖 Juicio del Experto AI", type="primary", use_container_width=True):
                    with st.spinner("Analizando..."):
                        st.session_state.ai_insight = generate_ai_insight(openai_key, selected_row)
                        st.rerun()
            
            if st.session_state.ai_insight:
                st.markdown(f'<div class="ai-box">{st.session_state.ai_insight}</div>', unsafe_allow_html=True)
                if st.button("🔄 Regenerar"): st.session_state.ai_insight = None; st.rerun()
        else:
            st.info("👈 Pulsa en una columna del mapa.")
            st.markdown("### 📋 Tabla de Posiciones")
            # Tabla limpia con Ranking
            cols_table = ['ranking', 'similarity', 'est_monthly_rent']
            st.dataframe(
                df_top[cols_table].set_index('ranking'),
                use_container_width=True
            )
        st.markdown('</div>', unsafe_allow_html=True)
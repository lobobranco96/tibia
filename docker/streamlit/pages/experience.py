import pandas as pd
import streamlit as st
from core.queries import experience_global_rank

# ===============================
# CONFIGURAÇÃO DA PÁGINA
# ===============================
st.set_page_config(
    page_title="Tibia - Ranking Global",
    layout="wide"
)

st.title("🏆 Tibia - Ranking Global de Players")

# ===============================
# BOTÃO REFRESH (limpa cache)
# ===============================
if st.sidebar.button("🔄 Refresh dados"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# CARGA DE DADOS
# ===============================
@st.cache_data(show_spinner="Carregando ranking global...")
def carregar_dados():
    df = experience_global_rank()

    # garante tipos corretos
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])

    return df

df = carregar_dados()

# ===============================
# SIDEBAR - FILTRO DE DATA
# ===============================
st.sidebar.header("🎛️ Filtros")

datas_disponiveis = sorted(df["snapshot_date"].dt.normalize().unique(), reverse=True)

data_selecionada = st.sidebar.selectbox(
    "📅 Data do Ranking",
    datas_disponiveis,
    format_func=lambda x: x.strftime("%Y-%m-%d")
)

# ===============================
# OUTROS FILTROS
# ===============================

# Filtro World
worlds = ["Todos"] + sorted(df["world"].unique().tolist())
world_selecionado = st.sidebar.selectbox("World", worlds)

# Filtro World Type
world_type = st.sidebar.multiselect(
    "World Type",
    options=sorted(df["world_type"].unique()),
    default=sorted(df["world_type"].unique())
)

# Filtro Vocation
vocation = st.sidebar.multiselect(
    "Vocation",
    options=sorted(df["vocation"].unique()),
    default=sorted(df["vocation"].unique())
)

# Filtro Top N
top_n = st.sidebar.selectbox(
    "Top Ranking",
    options=[10, 50, 100, 500, 1000],
    index=2
)

# ===============================
# APLICA FILTROS
# ===============================
df_filtrado = df[
    (df["snapshot_date"].dt.normalize() == data_selecionada) &
    (df["rank"] <= top_n) &
    (df["world_type"].isin(world_type)) &
    (df["vocation"].isin(vocation))
]

if world_selecionado != "Todos":
    df_filtrado = df_filtrado[df_filtrado["world"] == world_selecionado]

# ===============================
# MÉTRICAS
# ===============================
if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

col1, col2, col3, col4, col5, col6 = st.columns(6)

col1.metric("👥 Jogadores", len(df_filtrado))
col2.metric("📈 Level Máximo", int(df_filtrado["level"].max()))
col3.metric("📉 Level Mínimo", int(df_filtrado["level"].min()))
col4.metric("💠 Experiência Máxima", f"{df_filtrado['experience'].max():,}")
col5.metric("🌍 Mundos", df_filtrado["world"].nunique())
col6.metric("📅 Data do Ranking", data_selecionada.strftime("%Y-%m-%d"))

st.markdown("---")

# ===============================
# TABELA
# ===============================
st.subheader(f"📋 Ranking - Top {top_n}")

st.dataframe(
    df_filtrado.sort_values("rank"),
    use_container_width=True,
    hide_index=True
)

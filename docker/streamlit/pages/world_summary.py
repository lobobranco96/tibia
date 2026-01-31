import pandas as pd
import streamlit as st
from core.queries import world_summary

st.set_page_config(
    page_title="Tibia - Players por World",
    layout="wide"
)

st.title("🌍 Tibia - Players por World e Vocation")

@st.cache_data
def carregar_dados():
    df = world_summary()
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df

df = carregar_dados()

# ======================
# SIDEBAR - FILTROS
# ======================
st.sidebar.header("🎛️ Filtros")

# World Type
world_type = st.sidebar.multiselect(
    "World Type",
    options=sorted(df["world_type"].unique()),
    default=sorted(df["world_type"].unique())
)

# World
worlds = sorted(df["world"].unique())
world = st.sidebar.multiselect(
    "World",
    options=worlds,
    default=worlds
)

# Vocation
vocation = st.sidebar.multiselect(
    "Vocation",
    options=sorted(df["vocation"].unique()),
    default=sorted(df["vocation"].unique())
)

# ======================
# APLICA FILTROS
# ======================
df_filtrado = df[
    (df["world_type"].isin(world_type)) &
    (df["world"].isin(world)) &
    (df["vocation"].isin(vocation))
]

# AGREGAÇÕES
total_players = int(df_filtrado["players_count"].sum())

players_por_world = (
    df_filtrado
    .groupby("world", as_index=False)["players_count"]
    .sum()
    .sort_values("players_count", ascending=False)
)

players_por_vocation = (
    df_filtrado
    .groupby("vocation", as_index=False)["players_count"]
    .sum()
    .sort_values("players_count", ascending=False)
)

# MÉTRICAS
col1, col2, col3, col4 = st.columns(4)

col1.metric("Players Totais", total_players)
col2.metric("Worlds", players_por_world["world"].nunique())
col3.metric("Vocations", players_por_vocation["vocation"].nunique())
col4.metric(
    "Última Atualização",
    df_filtrado["updated_at"].max().strftime("%Y-%m-%d")
)

st.markdown("---")

# TABELAS

st.subheader("📋 Players por World")
st.dataframe(
    players_por_world,
    use_container_width=True,
    hide_index=True
)

st.subheader("📋 Players por Vocation")
st.dataframe(
    players_por_vocation,
    use_container_width=True,
    hide_index=True
)

st.markdown("---")

st.subheader("📋 Dados Detalhados")
st.dataframe(
    df_filtrado.sort_values("players_count", ascending=False),
    use_container_width=True,
    hide_index=True
)

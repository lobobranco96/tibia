import pandas as pd
import streamlit as st
from core.queries import experience_global_rank

st.set_page_config(
    page_title="Tibia - Ranking Global",
    layout="wide"
)

# BOTÃO REFRESH
if st.sidebar.button("🔄 Refresh dados"):
    st.cache_data.clear()
    st.rerun()  

st.title("🏆 Tibia - Ranking Global de Players")

@st.cache_data
def carregar_dados():
    df = experience_global_rank()
    #df = pd.read_csv("..\docs\csv_data\gold_experience_rank_global.csv")
    df = df.drop(df.columns[0], axis=1)
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df

df = carregar_dados()

# SIDEBAR 
st.sidebar.header("🎛️ Filtros")

# Filtro World
worlds = ["Todos"] + sorted(df["world"].unique().tolist())
world_selecionado = st.sidebar.selectbox(
    "World",
    worlds
)

# Filtro World Type
world_type = st.sidebar.multiselect(
    "World Type",
    options=df["world_type"].unique(),
    default=df["world_type"].unique()
)

# Filtro Vocation
vocation = st.sidebar.multiselect(
    "Vocation",
    options=df["vocation"].unique(),
    default=df["vocation"].unique()
)

# Filtro Top N
top_n = st.sidebar.selectbox(
    "Top Ranking",
    options=[10, 50, 100, 500, 1000],
    index=2  # default = Top 100
)


# APLICA FILTROS
df_filtrado = df[
    (df["rank"] <= top_n) &
    (df["world_type"].isin(world_type)) &
    (df["vocation"].isin(vocation))
]

if world_selecionado != "Todos":
    df_filtrado = df_filtrado[df_filtrado["world"] == world_selecionado]

# MÉTRICAS
col1, col2, col3, col4 = st.columns(4)

col1.metric("Jogadores", len(df_filtrado))
col2.metric("Level Máximo", int(df_filtrado["level"].max()))
col3.metric("Experiência Máxima", f"{df_filtrado['experience'].max():,}")
col4.metric("Mundos", df_filtrado["world"].nunique())

st.markdown("---")

# TABELA
st.subheader(f"📋 Ranking - Top {top_n}")

st.dataframe(
    df_filtrado.sort_values("rank"),
    use_container_width=True,
    hide_index=True
)
    
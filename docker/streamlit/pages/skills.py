import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Tibia - Ranking Global de Skills",
    layout="wide"
)

st.title("🛡️ Tibia - Ranking Global de Skills")

@st.cache_data
def carregar_dados():
    df = pd.read_csv("..\docs\csv_data\gold_skills_rank_global.csv")
    df = df.drop(df.columns[0], axis=1)
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    # cria rank por category (skill)
    df["rank"] = (
        df.groupby("skill_name")["skill_level"]
          .rank(method="dense", ascending=False)
          .astype(int)
    )

    return df

df = carregar_dados()

# SIDEBAR - FILTROS
st.sidebar.header("🎛️ Filtros")

# Category (Skill)
skill_selecionada = st.sidebar.selectbox(
    "Categoria",
    sorted(df["skill_name"].unique())
)

# World
worlds = ["Todos"] + sorted(df["world"].unique().tolist())
world_selecionado = st.sidebar.selectbox("World", worlds)

# Top N
top_n = st.sidebar.selectbox(
    "Top Ranking",
    options=[10, 50, 100, 500, 1000],
    index=2
)

# ======================
# APLICA FILTROS
# ======================
df_filtrado = df[df["skill_name"] == skill_selecionada]

if world_selecionado != "Todos":
    df_filtrado = df_filtrado[df_filtrado["world"] == world_selecionado]

df_filtrado = (
    df_filtrado
    .sort_values("rank")
    .head(top_n)
)

# ======================
# MÉTRICAS
# ======================
col1, col2, col3, col4 = st.columns(4)

col1.metric("Jogadores", len(df_filtrado))
col2.metric("Skill Máxima", int(df_filtrado["skill_level"].max()))
col3.metric("Mundos", df_filtrado["world"].nunique())
col4.metric("Última Atualização", df_filtrado["updated_at"].max().strftime("%Y-%m-%d"))

st.markdown("---")

# ======================
# TABELA
# ======================
st.subheader(f"📋 {skill_selecionada} — Top {top_n}")

st.dataframe(
    df_filtrado[
        ["rank", "name", "world", "skill_level", "updated_at"]
    ],
    use_container_width=True,
    hide_index=True
)
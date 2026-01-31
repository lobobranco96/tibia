import pandas as pd
import streamlit as st
from core.queries import skills_global_rank

st.set_page_config(
    page_title="Tibia - Ranking Global de Skills",
    layout="wide"
)

st.title("🛡️ Tibia - Ranking Global de Skills")

@st.cache_data
def carregar_dados():
    df = skills_global_rank()
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
col1, col2, col3, col4, col5 = st.columns(5)

if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

col1.metric("👥 Jogadores", len(df_filtrado))
col2.metric("📈 Skill Máxima", int(df_filtrado["skill_level"].max()))
col3.metric("📉 Skill Mínimo", int(df_filtrado["skill_level"].min()))
col4.metric("🌍 Mundos", df_filtrado["world"].nunique())
col5.metric(" Última Atualização", df_filtrado["updated_at"].max().strftime("%Y-%m-%d"))

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
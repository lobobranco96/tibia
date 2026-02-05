import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import plotly.express as px

# ===============================
# CONFIG STREAMLIT
# ===============================
st.set_page_config(
    page_title="Tibia - Player Experience Progression",
    layout="wide"
)

st.title("📈 Tibia – Evolução de Level e Experiência por Jogador")

# ===============================
# SPARK SESSION
# ===============================
@st.cache_resource
def create_spark():
    return SparkSession.builder.appName("tibia-lakehouse").getOrCreate()

spark = create_spark()


# ===============================
# LOAD DATA (SILVER VOCATION)
# ===============================

def load_data():
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .load("/content/drive/MyDrive/projetos/tibia/docs/csv_data/silver_vocation.csv")
    )
    df.createOrReplaceTempView("silver_vocation")

@st.cache_data
def load_gold_dataframe():
    spark_df = spark.sql(query)
    return spark_df.toPandas()


# ===============================
# QUERY (GOLD LOGIC)
query = """
WITH ordered_vocation AS (
    SELECT
        name,
        world,
        vocation,
        world_type,

        CAST(level AS INT) AS level,
        CAST(experience AS BIGINT) AS experience,
        CAST(start_date AS TIMESTAMP) AS start_date,
        CAST(is_current AS BOOLEAN) AS is_current,

        LAG(CAST(level AS INT)) OVER (
            PARTITION BY name
            ORDER BY start_date
        ) AS previous_level,

        LAG(CAST(experience AS BIGINT)) OVER (
            PARTITION BY name
            ORDER BY start_date
        ) AS previous_experience,

        LAG(start_date) OVER (
            PARTITION BY name
            ORDER BY start_date
        ) AS previous_start_date

    FROM silver_vocation
)

SELECT
    name,
    world,
    vocation,
    world_type,

    previous_level,
    level AS current_level,
    (level - previous_level) AS level_gain,

    previous_experience,
    experience AS current_experience,
    (experience - previous_experience) AS experience_gain,

    previous_start_date,
    start_date AS current_start_date,

    DATEDIFF(
        CAST(start_date AS DATE),
        CAST(previous_start_date AS DATE)
    ) AS days_between_updates,

    ROUND(
        (experience - previous_experience) /
        GREATEST(
            DATEDIFF(
                CAST(start_date AS DATE),
                CAST(previous_start_date AS DATE)
            ),
            1
        ),
        2
    ) AS avg_xp_per_day,

    is_current

FROM ordered_vocation
WHERE previous_level IS NOT NULL
ORDER BY name, current_start_date
"""

load_data()
df = load_gold_dataframe()

# SIDEBAR FILTERS
st.sidebar.header("🔎 Filtros")

world = st.sidebar.selectbox(
    "World",
    ["Todos"] + sorted(df["world"].dropna().unique())
)

vocations = sorted(df["vocation"].dropna().unique())

vocation_display = ["Todas"] + [v.title() for v in vocations]

vocation_selected = st.sidebar.selectbox(
    "Vocation",
    vocation_display
)

filtered_df = df.copy()

if world != "Todos":
    filtered_df = filtered_df[filtered_df["world"] == world]

if vocation_selected != "Todas":
    filtered_df = filtered_df[
        filtered_df["vocation"] == vocation_selected.lower()
    ]
player = st.sidebar.selectbox(
    "Player",
    sorted(filtered_df["name"].unique())
)

player_df = (
    filtered_df[filtered_df["name"] == player]
    .sort_values("current_start_date")
)

# KPIs

st.subheader(f"🎮 Player: {player}")

info_col1, info_col2, info_col3 = st.columns(3)

info_col1.info(f"🧙 **Vocation**: {player_df['vocation'].iloc[-1].title()}")
info_col2.info(f"🌍 **World**: {player_df['world'].iloc[-1]}")
info_col3.info(f"⚔️ **World Type**: {player_df['world_type'].iloc[-1]}")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Level Atual",
    int(player_df["current_level"].iloc[-1])
)

col2.metric(
    "XP Total Ganho",
    f"{int(player_df['experience_gain'].sum()):,}"
)

col3.metric(
    "XP Médio / Dia",
    f"{player_df['avg_xp_per_day'].mean():,.0f}"
)

col4.metric(
    "Dias Monitorados",
    int(player_df["days_between_updates"].sum())
)

# TABLE
st.subheader("📊 Histórico de Evolução")

player_df["experience_gain"] = player_df["experience_gain"].astype(float)
player_df["avg_xp_per_day"] = player_df["avg_xp_per_day"].astype(float)

# colunas que você quer mostrar
cols = [
    "previous_level",
    "current_level",
    "level_gain",
    "experience_gain",
    "avg_xp_per_day",
    "days_between_updates",
    "previous_start_date",
    "current_start_date"
]

st.dataframe(
    player_df[cols].style.format({
        "experience_gain": "{:,.2f}",
        "avg_xp_per_day": "{:,.2f}"
    }),
    use_container_width=True
)

# CHARTS
st.subheader("📈 Evolução ao Longo do Tempo")

fig_level = px.line(
    player_df,
    x="current_start_date",
    y="current_level",
    markers=True,
    title="Evolução de Level"
)

st.plotly_chart(fig_level, use_container_width=True)

fig_xp = px.bar(
    player_df,
    x="current_start_date",
    y="experience_gain",
    title="XP Ganha por Período"
)

st.plotly_chart(fig_xp, use_container_width=True)

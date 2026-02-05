import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import plotly.express as px

# ===============================
# CONFIG STREAMLIT
# ===============================
st.set_page_config(
    page_title="Tibia - Player Progression",
    layout="wide"
)

st.title("📈 Tibia – Evolução de Skills por Player")

# ===============================
# SPARK SESSION
# ===============================

# ===============================
# SPARK SESSION
# ===============================
@st.cache_resource
def create_spark():
    return SparkSession.builder.appName("tibia-lakehouse").getOrCreate()

spark = create_spark()

def load_data():
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .load("/content/drive/MyDrive/projetos/tibia/docs/csv_data/silver_skills.csv")
    )
    df.createOrReplaceTempView("silver_skills")

@st.cache_data
def load_gold_dataframe():
    spark_df = spark.sql(query)
    return spark_df.toPandas()


# ===============================
# QUERY
# ===============================
query = """
WITH ordered_progression AS (
    SELECT
        name,
        world,
        vocation,
        category,

        CAST(skill_level AS INT) AS skill_level,
        start_date,
        is_current,

        LAG(CAST(skill_level AS INT)) OVER (
            PARTITION BY name, category
            ORDER BY start_date
        ) AS previous_skill_level,

        LAG(start_date) OVER (
            PARTITION BY name, category
            ORDER BY start_date
        ) AS previous_start_date

    FROM silver_skills
)

SELECT
    name,
    world,
    vocation,
    category,

    previous_skill_level      AS skill_before,
    skill_level               AS skill_after,
    (skill_level - previous_skill_level) AS skill_gain,

    previous_start_date       AS from_date,
    start_date                AS to_date,

    DATEDIFF(
        CAST(start_date AS DATE),
        CAST(previous_start_date AS DATE)
    ) AS days_between_updates,

    ROUND(
        (skill_level - previous_skill_level) /
        NULLIF(
            DATEDIFF(
                CAST(start_date AS DATE),
                CAST(previous_start_date AS DATE)
            ),
            0
        ),
        2
    ) AS avg_skill_per_day,

    is_current

FROM ordered_progression
WHERE previous_skill_level IS NOT NULL
ORDER BY name, category, to_date
"""

load_data()
df = load_gold_dataframe()

# ===============================
# SIDEBAR FILTERS
# ===============================
st.sidebar.header("🔎 Filtros")

world = st.sidebar.selectbox(
    "World",
    ["Todos"] + sorted(df["world"].dropna().unique())
)

vocation = st.sidebar.selectbox(
    "Vocation",
    ["Todas"] + sorted(df["vocation"].dropna().unique())
)

category = st.sidebar.selectbox(
    "Categoria",
    ["Todas"] + sorted(df["category"].dropna().unique())
)

filtered_df = df.copy()

if world != "Todos":
    filtered_df = filtered_df[filtered_df["world"] == world]

if vocation != "Todas":
    filtered_df = filtered_df[filtered_df["vocation"] == vocation]

if category != "Todas":
    filtered_df = filtered_df[filtered_df["category"] == category]

player = st.sidebar.selectbox(
    "Player",
    sorted(filtered_df["name"].unique())
)

player_df = filtered_df[filtered_df["name"] == player]

# ===============================
# KPIs
# ===============================
st.subheader(f"🎮 Player: {player}")
player_info = player_df.iloc[0]

info_col1, info_col2, info_col3 = st.columns(3)

info_col1.metric("🌍 World", player_info["world"])
info_col2.metric("🧙 Vocation", player_info["vocation"])

col1, col2, col3 = st.columns(3)

col1.metric(
    "Skill Atual",
    int(player_df["skill_after"].max())
)

col2.metric(
    "Skill Total Ganha",
    int(player_df["skill_gain"].sum())
)

col3.metric(
    "Média Skill / Dia",
    round(player_df["avg_skill_per_day"].mean(), 2)
)

# ===============================
# TABLE
# ===============================
st.subheader("📊 Histórico de Evolução")

st.dataframe(
    player_df[
        [
            "category",
            "skill_before",
            "skill_after",
            "skill_gain",
            "days_between_updates",
            "avg_skill_per_day",
            "from_date",
            "to_date"
        ]
    ],
    use_container_width=True
)

# ===============================
# CHART
# ===============================
st.subheader("📈 Evolução ao Longo do Tempo")

fig = px.line(
    player_df,
    x="to_date",
    y="skill_after",
    color="category",
    markers=True,
    title="Evolução de Skill por Data"
)

st.plotly_chart(fig, use_container_width=True)

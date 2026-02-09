import pandas as pd
import streamlit as st
from core.queries import world_summary

# ===============================
# CONFIGURATION
# ===============================
st.set_page_config(
    page_title="Tibia - Players by World",
    layout="wide"
)

st.title("🌍 Tibia - Player Distribution by Vocation and World")

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner="Loading world summary...")
def load_data():
    df = world_summary()
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    # Normalize text
    df["world"] = df["world"].str.title()
    df["vocation"] = df["vocation"].str.title()
    df["world_type"] = df["world_type"].str.title()
    return df

df = load_data()

# ===============================
# SIDEBAR FILTERS
# ===============================
st.sidebar.header("🎛️ Filters")

# World Type
world_type = st.sidebar.multiselect(
    "World Type",
    options=sorted(df["world_type"].unique()),
    default=sorted(df["world_type"].unique())
)

# World
worlds = sorted(df["world"].unique())
world_selected = st.sidebar.multiselect(
    "World",
    options=worlds,
    default=worlds
)

# Vocation
vocations = sorted(df["vocation"].unique())
vocation_selected = st.sidebar.multiselect(
    "Vocation",
    options=vocations,
    default=vocations
)

# ===============================
# APPLY FILTERS
# ===============================
df_filtered = df[
    (df["world_type"].isin(world_type)) &
    (df["world"].isin(world_selected)) &
    (df["vocation"].isin(vocation_selected))
]

# ===============================
# AGGREGATIONS
# ===============================
total_players = int(df_filtered["players_count"].sum())

players_by_world = (
    df_filtered.groupby("world", as_index=False)["players_count"]
    .sum()
    .sort_values("players_count", ascending=False)
)

players_by_vocation = (
    df_filtered.groupby("vocation", as_index=False)["players_count"]
    .sum()
    .sort_values("players_count", ascending=False)
)

# ===============================
# METRICS
# ===============================
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Players", total_players)
col2.metric("Worlds", players_by_world["world"].nunique())
col3.metric("Vocations", players_by_vocation["vocation"].nunique())
col4.metric(
    "Last Update",
    df_filtered["updated_at"].max().strftime("%Y-%m-%d %H:%M:%S")
)

st.markdown("---")

# ===============================
# TABLES
# ===============================
st.subheader("📋 Players by World")
st.dataframe(
    players_by_world,
    use_container_width=True,
    hide_index=True
)

st.subheader("📋 Players by Vocation")
st.dataframe(
    players_by_vocation,
    use_container_width=True,
    hide_index=True
)

st.markdown("---")

st.subheader("📋 Detailed Data")

df_filtered["updated_at"] = df_filtered["updated_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
st.dataframe(
    df_filtered.sort_values("players_count", ascending=False),
    use_container_width=True,
    hide_index=True
)

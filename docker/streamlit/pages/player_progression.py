import pandas as pd
import streamlit as st
import plotly.express as px
from core.queries import player_progression

# ===============================
# STREAMLIT CONFIG
# ===============================
st.set_page_config(
    page_title="Tibia - Player Experience Progression",
    layout="wide"
)

st.title("📈 Tibia – Player Level & Experience Progression")

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner="Loading player progression...")
def load_progression():
    df = player_progression()
    df["previous_start_date"] = pd.to_datetime(df["previous_start_date"])
    df["current_start_date"] = pd.to_datetime(df["current_start_date"])
    return df

df = load_progression()

# ===============================
# SIDEBAR - FILTERS
# ===============================
st.sidebar.header("🔎 Filters")

# World
world = st.sidebar.selectbox(
    "World",
    ["All"] + sorted(df["world"].dropna().unique()),
    key="world_filter"
)

# Vocation
vocations = sorted(df["vocation"].dropna().unique())
vocation_display = ["All"] + [v.title() for v in vocations]
vocation_selected = st.sidebar.selectbox(
    "Vocation",
    vocation_display,
    key="vocation_filter"
)

# Player search
player_search = st.sidebar.text_input(
    "🔍 Search Player",
    value="",
    key="player_search"
).strip().lower()

# ===============================
# FILTER DATA
# ===============================
filtered_df = df.copy()

if world != "All":
    filtered_df = filtered_df[filtered_df["world"] == world]

if vocation_selected != "All":
    filtered_df = filtered_df[filtered_df["vocation"].str.lower() == vocation_selected.lower()]

if player_search:
    filtered_df = filtered_df[filtered_df["name"].str.lower().str.contains(player_search, na=False)]

# Stop if no data
if filtered_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

# Select player from filtered data
player = st.sidebar.selectbox(
    "Player",
    sorted(filtered_df["name"].unique()),
    key="player_select"
)

# Filter dataframe for selected player
player_df = filtered_df[filtered_df["name"] == player].sort_values("current_start_date")

# ===============================
# KPIs
# ===============================
st.subheader(f"🎮 Player: {player}")

info_col1, info_col2, info_col3 = st.columns(3)
info_col1.info(f"🧙 **Vocation**: {player_df['vocation'].iloc[-1].title()}")
info_col2.info(f"🌍 **World**: {player_df['world'].iloc[-1]}")
info_col3.info(f"⚔️ **World Type**: {player_df['world_type'].iloc[-1]}")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Current Level", int(player_df["current_level"].iloc[-1]))
col2.metric("Total XP Gained", f"{int(player_df['experience_gain'].sum()):,}")
col3.metric("Average XP / Day", f"{player_df['avg_xp_per_day'].mean():,.0f}")
col4.metric("Days Monitored", int(player_df["days_between_updates"].sum()))

# ===============================
# DATA TABLE
# ===============================
st.subheader("📊 Progress History")

player_df["experience_gain"] = player_df["experience_gain"].astype(float)
player_df["avg_xp_per_day"] = player_df["avg_xp_per_day"].astype(float)

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

player_df["previous_start_date"] = player_df["previous_start_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
player_df["current_start_date"] = player_df["current_start_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

st.dataframe(
    player_df[cols].style.format({
        "experience_gain": "{:,.2f}",
        "avg_xp_per_day": "{:,.2f}"
    }),
    use_container_width=True
)

# ===============================
# CHARTS
# ===============================
st.subheader("📈 Progress Over Time")

fig_level = px.line(
    player_df,
    x="current_start_date",
    y="current_level",
    markers=True,
    title="Level Progression"
)
st.plotly_chart(fig_level, use_container_width=True)

fig_xp = px.bar(
    player_df,
    x="current_start_date",
    y="experience_gain",
    title="XP Gained per Period"
)
st.plotly_chart(fig_xp, use_container_width=True)

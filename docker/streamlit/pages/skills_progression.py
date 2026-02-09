import pandas as pd
import streamlit as st
import plotly.express as px
from core.queries import skill_progression

# ===============================
# STREAMLIT CONFIG
# ===============================
st.set_page_config(
    page_title="Tibia - Player Skill Progression",
    layout="wide"
)

st.title("📈 Tibia – Player Skill Progression")

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner="Loading skill progression...")
def load_progression():
    df = skill_progression()
    df["from_date"] = pd.to_datetime(df["from_date"])
    df["to_date"] = pd.to_datetime(df["to_date"])
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
    key="world_select_skills"
)

# Vocation
vocations = sorted(df["vocation"].dropna().unique())
vocation_display = ["All"] + [v.title() for v in vocations]
vocation_selected = st.sidebar.selectbox(
    "Vocation",
    vocation_display,
    key="vocation_select_skills"
)

# Category
categories = sorted(df["category"].dropna().unique())
category_selected = st.sidebar.selectbox(
    "Category",
    ["All"] + categories,
    key="category_select_skills"
)

# Player search
player_search = st.sidebar.text_input(
    "🔍 Search Player",
    value="",
    key="player_search_skills"
).strip().lower()

# ===============================
# FILTER DATA
# ===============================
filtered_df = df.copy()

if world != "All":
    filtered_df = filtered_df[filtered_df["world"] == world]

if vocation_selected != "All":
    filtered_df = filtered_df[filtered_df["vocation"].str.lower() == vocation_selected.lower()]

if category_selected != "All":
    filtered_df = filtered_df[filtered_df["category"] == category_selected]

if player_search:
    filtered_df = filtered_df[filtered_df["name"].str.lower().str.contains(player_search)]

# Stop if no data
if filtered_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

# ===============================
# SELECT PLAYER
# ===============================
player = st.sidebar.selectbox(
    "Player",
    sorted(filtered_df["name"].unique()),
    key="player_select_skills"
)

player_df = filtered_df[filtered_df["name"] == player].sort_values("to_date")

if player_df.empty:
    st.warning("No data found for the selected player.")
    st.stop()

# ===============================
# KPIs
# ===============================
st.subheader(f"🎮 Player: {player}")

info_col1, info_col2, info_col3 = st.columns(3)
info_col1.info(f"🌍 World: {player_df['world'].iloc[-1].title()}")
info_col2.info(f"🧙 Vocation: {player_df['vocation'].iloc[-1].title()}")
info_col3.info(f"🛡️ Category: {player_df['category'].iloc[-1]}")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Current Skill", int(player_df["skill_after"].max()))
col2.metric("Total Skill Gained", int(player_df["skill_gain"].sum()))
col3.metric("Average Skill / Day", round(player_df["avg_skill_per_day"].mean(), 2))
col4.metric("Days Monitored", int(player_df["days_between_updates"].sum()))

# ===============================
# DATA TABLE
# ===============================
st.subheader("📊 Progress History")

player_df["skill_gain"] = player_df["skill_gain"].astype(float)
player_df["avg_skill_per_day"] = player_df["avg_skill_per_day"].astype(float)

cols = [
    "category",
    "skill_before",
    "skill_after",
    "skill_gain",
    "days_between_updates",
    "avg_skill_per_day",
    "from_date",
    "to_date"
]
player_df["from_date"] = player_df["from_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
player_df["to_date"] = player_df["to_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

st.dataframe(
    player_df[cols].style.format({
        "skill_gain": "{:,.2f}",
        "avg_skill_per_day": "{:,.2f}"
    }),
    use_container_width=True
)

# ===============================
# CHARTS
# ===============================
st.subheader("📈 Progress Over Time")

fig_skill = px.line(
    player_df,
    x="to_date",
    y="skill_after",
    color="category",
    markers=True,
    title="Skill Progression Over Time"
)

st.plotly_chart(fig_skill, use_container_width=True)

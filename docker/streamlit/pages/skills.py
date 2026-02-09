import pandas as pd
import streamlit as st
from core.queries import skills_global_rank

# ===============================
# CONFIGURATION
# ===============================
st.set_page_config(
    page_title="Tibia - Global Skill Ranking",
    layout="wide"
)

st.title("🛡️ Tibia - Global Skill Ranking by Category")

# ===============================
# REFRESH BUTTON
# ===============================
if st.sidebar.button("🔄 Refresh data"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner="Loading skill ranking...")
def load_data():
    df = skills_global_rank()

    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["rank"] = df["rank"].astype(int)

    # Normalize text
    df["world"] = df["world"].str.title()
    df["vocation"] = df["vocation"].str.title()
    df["skill_name"] = df["skill_name"].str.title()

    return df

df = load_data()

# ===============================
# SIDEBAR FILTERS
# ===============================
st.sidebar.header("🎛️ Filters")

# Skill / Category
skill_selected = st.sidebar.selectbox(
    "Category",
    sorted(df["skill_name"].unique())
)

# World
worlds = ["All"] + sorted(df["world"].unique())
world_selected = st.sidebar.selectbox("World", worlds)

# Top N
top_n = st.sidebar.selectbox(
    "Top Ranking",
    options=[10, 50, 100, 500, 1000],
    index=2
)

# Player search
player_search = st.sidebar.text_input(
    "🔍 Search Player",
    value=""
).strip().lower()

# ===============================
# APPLY FILTERS
# ===============================
filtered_df = df[
    (df["skill_name"] == skill_selected) &
    (df["rank"] <= top_n)
]

if world_selected != "All":
    filtered_df = filtered_df[filtered_df["world"] == world_selected]

if player_search:
    filtered_df = filtered_df[filtered_df["name"].str.lower().str.contains(player_search)]

if filtered_df.empty:
    st.warning("No data found for selected filters.")
    st.stop()

# ===============================
# METRICS
# ===============================
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("👥 Players", len(filtered_df))
col2.metric("📈 Max Skill", int(filtered_df["skill_level"].max()))
col3.metric("📉 Min Skill", int(filtered_df["skill_level"].min()))
col4.metric("🌍 Worlds", filtered_df["world"].nunique())
col5.metric("🕒 Last Update", filtered_df["updated_at"].max().strftime("%Y-%m-%d %H:%M:%S"))

st.markdown("---")

# ===============================
# TABLE
# ===============================
st.subheader(f"📋 {skill_selected} — Top {top_n}")

df_display = filtered_df.copy()
df_display["updated_at"] = df_display["updated_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

st.dataframe(
    df_display[["rank", "name", "world", "vocation", "skill_level", "updated_at"]]
    .sort_values("rank"),
    use_container_width=True,
    hide_index=True
)

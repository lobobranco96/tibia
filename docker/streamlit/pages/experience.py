import pandas as pd
import streamlit as st
from core.queries import experience_global_rank

# ===============================
# STREAMLIT CONFIG
# ===============================
st.set_page_config(
    page_title="Tibia - Global Player Ranking",
    layout="wide"
)

st.title("🏆 Tibia - Global Player Ranking")

# ===============================
# REFRESH BUTTON
# ===============================
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner="Loading global ranking...")
def load_data():
    df = experience_global_rank()

    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["rank"] = df["rank"].astype(int)

    # normalize text
    df["world"] = df["world"].str.title()
    df["vocation"] = df["vocation"].str.title()
    df["world_type"] = df["world_type"].str.title()

    return df

df = load_data()

# ===============================
# SIDEBAR FILTERS
# ===============================
st.sidebar.header("🎛️ Filters")

# Snapshot Date
available_dates = sorted(df["snapshot_date"].dt.normalize().unique(), reverse=True)
if len(available_dates) == 0:
    st.warning("No snapshots available yet.")
    st.stop()

selected_date = st.sidebar.selectbox(
    "📅 Snapshot Date",
    available_dates,
    format_func=lambda x: x.strftime("%Y-%m-%d")
)

# World
worlds = ["All"] + sorted(df["world"].unique().tolist())
selected_world = st.sidebar.selectbox("World", worlds)

# World Type
world_type = st.sidebar.multiselect(
    "World Type",
    options=sorted(df["world_type"].unique()),
    default=sorted(df["world_type"].unique())
)

# Vocation
vocation = st.sidebar.multiselect(
    "Vocation",
    options=sorted(df["vocation"].unique()),
    default=sorted(df["vocation"].unique())
)

# Top N
top_n = st.sidebar.selectbox(
    "Top Ranking",
    options=[10, 50, 100, 500, 1000],
    index=2
)

# Player search
player_search = st.sidebar.text_input("🔍 Search Player")

# ===============================
# APPLY FILTERS
# ===============================
df_filtered = df[
    (df["snapshot_date"].dt.normalize() == selected_date) &
    (df["rank"] <= top_n) &
    (df["world_type"].isin(world_type)) &
    (df["vocation"].isin(vocation))
]

if selected_world != "All":
    df_filtered = df_filtered[df_filtered["world"] == selected_world]

if player_search:
    df_filtered = df_filtered[df_filtered["name"].str.contains(player_search, case=False, na=False)]

if df_filtered.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

df_filtered = df_filtered.copy()
df_filtered["experience"] = df_filtered["experience"].astype(float)

# ===============================
# KPIs
# ===============================
col1, col2, col3, col4, col5, col6 = st.columns(6)

col1.metric("👥 Players", len(df_filtered))
col2.metric("📈 Max Level", int(df_filtered["level"].max()))
col3.metric("📉 Min Level", int(df_filtered["level"].min()))
col4.metric("💠 Max Experience", f"{df_filtered['experience'].max():,}")
col5.metric("🌍 Worlds", df_filtered["world"].nunique())
col6.metric("📅 Snapshot Date", selected_date.strftime("%Y-%m-%d"))

st.markdown("---")

# ===============================
# DATA TABLE
# ===============================
st.subheader(f"📋 Ranking - Top {top_n}")

df_display = df_filtered.drop(columns=["snapshot_date"]).sort_values("rank")
df_display["updated_at"] = df_display["updated_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

st.dataframe(
    df_display.style.format({"experience": "{:,.2f}"}),
    use_container_width=True,
    hide_index=True
)

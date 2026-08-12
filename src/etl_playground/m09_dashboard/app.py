"""Module 09: Streamlit Dashboard — "QuickSight" equivalent.

Provides 5 analytical visuals with Region and Date Range filters.
Queries the Parquet lake directly via DuckDB.

Usage:
    uv run streamlit run src/etl_playground/m09_dashboard/app.py
"""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import streamlit as st

from etl_playground.m09_dashboard.pages import (
    render_completion_by_device,
    render_kpi_cards,
    render_launch_success,
    render_top_titles,
    render_trend_chart,
)

# ── Page config ──
st.set_page_config(
    page_title="Content Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Content Analytics Dashboard")
st.caption(
    "ETL Playground — Streamlit ≈ QuickSight | Data: Synthetic content engagement events"
)


# ── Database connection ──
@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Connect to warehouse or fall back to in-memory for lake queries."""
    wh_path = Path("data/warehouse.duckdb")
    if wh_path.exists():
        return duckdb.connect(str(wh_path), read_only=True)
    return duckdb.connect(":memory:")


conn = get_connection()

# ── Filters ──
st.sidebar.header("Filters")

regions = ["UK", "US", "DE", "JP"]
selected_regions = st.sidebar.multiselect("Regions", regions, default=regions)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(date.today() - timedelta(days=14), date.today()),
    min_value=date(2020, 1, 1),
    max_value=date.today(),
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date.today() - timedelta(days=14)
    end_date = date.today()

region_filter = ", ".join(f"'{r}'" for r in selected_regions)

# Parquet lake path
PARQUET_PATH = "data/curated/viewing_events_enriched/**/*.parquet"

# ── Row 1: KPI Cards ──
st.subheader("Key Metrics")
render_kpi_cards(conn, PARQUET_PATH, start_date, end_date, region_filter)

# ── Row 2: Trends + Completion by Device ──
st.subheader("Engagement Trends")
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Weekly Engagement Trend**")
    render_trend_chart(conn, PARQUET_PATH, start_date, end_date, region_filter)

with col_right:
    st.markdown("**Completion Rate by Device Type**")
    render_completion_by_device(conn, PARQUET_PATH, start_date, end_date, region_filter)

# ── Row 3: Top Titles + Launch Success ──
st.subheader("Content Performance")
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.markdown("**Top 10 Titles by Watch Hours**")
    render_top_titles(conn, PARQUET_PATH, start_date, end_date, region_filter)

with col_right2:
    st.markdown("**Launch Success Rate by Region**")
    render_launch_success(conn, region_filter)

# ── Footer ──
st.divider()
st.caption(
    "📊 ETL Playground — Content Analytics Dashboard | "
    "Data: Synthetic content engagement events | "
    "Stack: PySpark → Parquet → DuckDB → Streamlit"
)

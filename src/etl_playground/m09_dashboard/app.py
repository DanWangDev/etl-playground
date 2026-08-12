"""Module 09: Streamlit Dashboard — "QuickSight" equivalent.

Provides 5 analytical visuals with Region and Date Range filters.
Queries the DuckDB warehouse (or the Parquet lake directly).

Usage:
    uv run streamlit run src/etl_playground/m09_dashboard/app.py
"""

from datetime import date, timedelta
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

# ── Page config ──
st.set_page_config(
    page_title="Content Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Content Analytics Dashboard")
st.caption("ETL Playground — Streamlit ≈ QuickSight | Data: Synthetic content engagement events")

# ── Database connection ──
@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Connect to warehouse or lake Parquet."""
    wh_path = Path("data/warehouse.duckdb")
    if wh_path.exists():
        return duckdb.connect(str(wh_path), read_only=True)
    return duckdb.connect(":memory:")


conn = get_connection()

# ── Filters ──
st.sidebar.header("Filters")

regions = ["UK", "US", "DE", "JP"]
selected_regions = st.sidebar.multiselect(
    "Regions", regions, default=regions
)

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

# Build region filter SQL
region_filter = ", ".join(f"'{r}'" for r in selected_regions)

# ── Use Parquet lake if warehouse not available ──
USE_LAKE = True  # Query Parquet directly for simplicity
PARQUET_PATH = "data/curated/viewing_events_enriched/**/*.parquet"


def query_lake(sql: str) -> pd.DataFrame:
    """Run a SQL query against the Parquet lake."""
    return conn.execute(sql).df()


# ── Row 1: KPI Cards ──
st.subheader("Key Metrics")

kpi_sql = f"""
SELECT
    COUNT(*) AS total_views,
    ROUND(SUM(watch_minutes) / 60.0, 0) AS total_watch_hours,
    ROUND(AVG(completion_rate), 2) AS avg_completion,
    COUNT(DISTINCT content_id) AS unique_content
FROM read_parquet('{PARQUET_PATH}')
WHERE region IN ({region_filter})
  AND event_date BETWEEN '{start_date}' AND '{end_date}'
"""
kpi = query_lake(kpi_sql)

if not kpi.empty:
    col1, col2, col3, col4 = st.columns(4)
    row = kpi.iloc[0]
    col1.metric("Total Views", f"{row['total_views']:,.0f}")
    col2.metric("Total Watch Hours", f"{row['total_watch_hours']:,.0f}")
    col3.metric("Avg Completion Rate", f"{row['avg_completion']:.0%}")
    col4.metric("Unique Content", f"{row['unique_content']:,.0f}")
else:
    st.warning("No data found. Run the ETL pipeline first (Modules 01-04).")

# ── Row 2: Weekly Trend + Completion by Device ──
st.subheader("Engagement Trends")
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Weekly Engagement Trend**")
    trend_sql = f"""
    SELECT
        event_date,
        COUNT(*) AS views,
        ROUND(SUM(watch_minutes) / 60.0, 1) AS watch_hours
    FROM read_parquet('{PARQUET_PATH}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY event_date
    ORDER BY event_date
    """
    trend_df = query_lake(trend_sql)
    if not trend_df.empty:
        chart = (
            alt.Chart(trend_df)
            .mark_bar()
            .encode(
                x=alt.X("event_date:T", title="Date"),
                y=alt.Y("watch_hours:Q", title="Watch Hours"),
                tooltip=["event_date", "views", "watch_hours"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No trend data available")

with col_right:
    st.markdown("**Completion Rate by Device Type**")
    device_sql = f"""
    SELECT
        device_type,
        ROUND(AVG(completion_rate), 2) AS avg_completion,
        COUNT(*) AS events
    FROM read_parquet('{PARQUET_PATH}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY device_type
    ORDER BY events DESC
    """
    device_df = query_lake(device_sql)
    if not device_df.empty:
        chart = (
            alt.Chart(device_df)
            .mark_bar()
            .encode(
                x=alt.X("device_type:N", title="Device Type"),
                y=alt.Y("avg_completion:Q", title="Avg Completion Rate"),
                color=alt.Color("device_type:N", legend=None),
                tooltip=["device_type", "avg_completion", "events"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No device data available")

# ── Row 3: Top Titles + Launch Success ──
st.subheader("Content Performance")
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.markdown("**Top 10 Titles by Watch Hours**")
    top_sql = f"""
    SELECT
        COALESCE(title, content_id) AS title,
        ROUND(SUM(watch_minutes) / 60.0, 1) AS watch_hours,
        COUNT(*) AS views
    FROM read_parquet('{PARQUET_PATH}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY title, content_id
    ORDER BY watch_hours DESC
    LIMIT 10
    """
    top_df = query_lake(top_sql)
    if not top_df.empty:
        st.dataframe(
            top_df,
            column_config={
                "title": "Title",
                "watch_hours": st.column_config.NumberColumn("Watch Hours", format="%.1f"),
                "views": st.column_config.NumberColumn("Views", format="%,d"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No content data available")

with col_right2:
    st.markdown("**Launch Success Rate by Region**")
    launch_sql = f"""
    SELECT
        region,
        COUNT(*) AS total_launches,
        SUM(CASE WHEN launch_status = 'on_time' THEN 1 ELSE 0 END) AS on_time,
        SUM(CASE WHEN launch_status = 'delayed' THEN 1 ELSE 0 END) AS delayed,
        SUM(CASE WHEN launch_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
    FROM read_csv('data/raw/content_launch/launch.csv', header=true)
    WHERE region IN ({region_filter})
    GROUP BY region
    ORDER BY region
    """
    launch_df = query_lake(launch_sql)
    if not launch_df.empty:
        launch_df["success_rate"] = (
            launch_df["on_time"] / launch_df["total_launches"] * 100
        )
        st.dataframe(
            launch_df,
            column_config={
                "region": "Region",
                "total_launches": "Total",
                "on_time": "On Time",
                "delayed": "Delayed",
                "cancelled": "Cancelled",
                "success_rate": st.column_config.NumberColumn("Success Rate", format="%.1f%%"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No launch data available")

# ── Footer ──
st.divider()
st.caption(
    "📊 ETL Playground — Content Analytics Dashboard | "
    "Data: Synthetic content engagement events | "
    "Stack: PySpark → Parquet → DuckDB → Streamlit"
)

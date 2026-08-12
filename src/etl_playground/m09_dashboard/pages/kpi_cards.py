"""KPI card component for the Streamlit dashboard.

Displays: Total Watch Hours, Total Views, Avg Completion Rate, Unique Content.
Queries the Parquet lake directly via DuckDB.
"""

from datetime import date

import duckdb
import streamlit as st


def render_kpi_cards(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: str,
    start_date: date,
    end_date: date,
    region_filter: str,
) -> None:
    """Render four KPI metric cards in a row."""
    sql = f"""
    SELECT
        COUNT(*) AS total_views,
        ROUND(SUM(watch_minutes) / 60.0, 0) AS total_watch_hours,
        ROUND(AVG(completion_rate), 2) AS avg_completion,
        COUNT(DISTINCT content_id) AS unique_content
    FROM read_parquet('{parquet_path}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    """

    kpi = conn.execute(sql).df()

    if kpi.empty:
        st.warning("No data found for selected filters.")
        return

    col1, col2, col3, col4 = st.columns(4)
    row = kpi.iloc[0]
    col1.metric("Total Views", f"{row['total_views']:,.0f}")
    col2.metric("Total Watch Hours", f"{row['total_watch_hours']:,.0f}")
    col3.metric("Avg Completion Rate", f"{row['avg_completion']:.0%}")
    col4.metric("Unique Content", f"{row['unique_content']:,.0f}")

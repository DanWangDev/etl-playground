"""Launch Success Rate by Region table for the Streamlit dashboard."""

import duckdb
import streamlit as st


def render_launch_success(
    conn: duckdb.DuckDBPyConnection,
    region_filter: str,
) -> None:
    """Render a data table showing launch success rates by region.

    Note: This queries raw CSV (content_launch) since launch data
    is a reference table, not part of the event stream.
    """
    sql = f"""
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

    df = conn.execute(sql).df()

    if df.empty:
        st.info("No launch data available.")
        return

    df["success_rate"] = df["on_time"] / df["total_launches"] * 100

    st.dataframe(
        df,
        column_config={
            "region": "Region",
            "total_launches": "Total",
            "on_time": "On Time",
            "delayed": "Delayed",
            "cancelled": "Cancelled",
            "success_rate": st.column_config.NumberColumn(
                "Success Rate", format="%.1f%%"
            ),
        },
        hide_index=True,
        use_container_width=True,
    )

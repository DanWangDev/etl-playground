"""Completion Rate by Device Type chart for the Streamlit dashboard."""

from datetime import date

import altair as alt
import duckdb
import streamlit as st


def render_completion_by_device(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: str,
    start_date: date,
    end_date: date,
    region_filter: str,
) -> None:
    """Render a bar chart showing avg completion rate by device type."""
    sql = f"""
    SELECT
        device_type,
        ROUND(AVG(completion_rate), 2) AS avg_completion,
        COUNT(*) AS events
    FROM read_parquet('{parquet_path}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY device_type
    ORDER BY events DESC
    """

    df = conn.execute(sql).df()

    if df.empty:
        st.info("No device data available for the selected period.")
        return

    chart = (
        alt.Chart(df)
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

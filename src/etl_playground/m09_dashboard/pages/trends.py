"""Weekly Engagement Trend chart for the Streamlit dashboard.

Displays a bar chart of watch hours by date.
"""

from datetime import date

import altair as alt
import duckdb
import streamlit as st


def render_trend_chart(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: str,
    start_date: date,
    end_date: date,
    region_filter: str,
) -> None:
    """Render a bar chart showing daily watch hours over the selected period."""
    sql = f"""
    SELECT
        event_date,
        COUNT(*) AS views,
        ROUND(SUM(watch_minutes) / 60.0, 1) AS watch_hours
    FROM read_parquet('{parquet_path}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY event_date
    ORDER BY event_date
    """

    df = conn.execute(sql).df()

    if df.empty:
        st.info("No trend data available for the selected period.")
        return

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("event_date:T", title="Date"),
            y=alt.Y("watch_hours:Q", title="Watch Hours"),
            tooltip=["event_date", "views", "watch_hours"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

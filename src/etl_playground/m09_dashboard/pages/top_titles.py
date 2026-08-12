"""Top 10 Titles by Watch Hours table for the Streamlit dashboard."""

from datetime import date

import duckdb
import streamlit as st


def render_top_titles(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: str,
    start_date: date,
    end_date: date,
    region_filter: str,
) -> None:
    """Render a data table showing the top 10 titles by watch hours."""
    sql = f"""
    SELECT
        COALESCE(title, content_id) AS title,
        ROUND(SUM(watch_minutes) / 60.0, 1) AS watch_hours,
        COUNT(*) AS views
    FROM read_parquet('{parquet_path}')
    WHERE region IN ({region_filter})
      AND event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY title, content_id
    ORDER BY watch_hours DESC
    LIMIT 10
    """

    df = conn.execute(sql).df()

    if df.empty:
        st.info("No content data available for the selected period.")
        return

    st.dataframe(
        df,
        column_config={
            "title": "Title",
            "watch_hours": st.column_config.NumberColumn("Watch Hours", format="%.1f"),
            "views": st.column_config.NumberColumn("Views", format="%,d"),
        },
        hide_index=True,
        use_container_width=True,
    )

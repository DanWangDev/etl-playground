"""Module 07 Exercise: Lake Queries — DuckDB as "Athena".

Runs analytical queries directly against Parquet files in the data lake.
Demonstrates predicate pushdown, partition pruning, and scan efficiency.

Usage:
    uv run python -m etl_playground.m07_lake_queries.exercise
"""

import duckdb

from etl_playground.m07_lake_queries.queries import (
    COMPLETION_BY_DEVICE_SQL,
    REGION_BREAKDOWN_SQL,
    TOP_CONTENT_UK_SQL,
    WEEKLY_TREND_SQL,
    run_lake_query,
)
from etl_playground.shared.logging import etl_context


def main() -> None:
    with etl_context("M07_Lake_Queries") as (log, run):
        log.header("ETL Playground — Lake Queries (DuckDB ≈ Athena)")

        conn = duckdb.connect(":memory:")
        conn.execute("SET enable_progress_bar = false")

        # Path to enriched Parquet in the curated zone
        parquet_path = "data/curated/viewing_events_enriched"

        log.info(f"Querying Parquet lake at: {parquet_path}/**/*.parquet")
        log.info("DuckDB handles: partition pruning, column pruning, predicate pushdown")
        log.info("")

        # ── Query 1: Top Content in UK ──
        log.stage("Query 1/4: Top 20 Content by Watch Hours (UK)")
        run.start_stage("top_content_uk")
        sql1 = TOP_CONTENT_UK_SQL.format(parquet_path=parquet_path)
        rows1, m1 = run_lake_query(conn, sql1, "top_content_uk", log)
        for row in rows1[:5]:
            log.detail(f"  {row[0][:12]:12s} | {str(row[1])[:30]:30s} | {row[2]:8.1f}h | {row[3]:5d} views")
        if len(rows1) > 5:
            log.detail(f"  ... and {len(rows1) - 5} more")
        run.end_stage("top_content_uk", output_rows=len(rows1))

        # ── Query 2: Weekly Trend ──
        log.stage("Query 2/4: Weekly Engagement Trend by Region")
        run.start_stage("weekly_trend")
        sql2 = WEEKLY_TREND_SQL.format(parquet_path=parquet_path)
        rows2, m2 = run_lake_query(conn, sql2, "weekly_trend", log)
        # Show sample
        for row in rows2[:4]:
            log.detail(f"  {row[0]} | {row[1]:2s} | {row[2]:5d} views | {row[3]:6.0f}h")
        log.detail(f"  ... ({len(rows2)} total rows)")
        run.end_stage("weekly_trend", output_rows=len(rows2))

        # ── Query 3: Completion by Device ──
        log.stage("Query 3/4: Completion Rate by Device Type")
        run.start_stage("completion_by_device")
        sql3 = COMPLETION_BY_DEVICE_SQL.format(parquet_path=parquet_path)
        rows3, m3 = run_lake_query(conn, sql3, "completion_by_device", log)
        for row in rows3:
            log.detail(f"  {row[0]:10s} | {row[1]:5d} events | {row[2]:.2f} avg completion | {row[3]:5d} completed")
        run.end_stage("completion_by_device", output_rows=len(rows3))

        # ── Query 4: Region Breakdown ──
        log.stage("Query 4/4: Region Breakdown")
        run.start_stage("region_breakdown")
        sql4 = REGION_BREAKDOWN_SQL.format(parquet_path=parquet_path)
        rows4, m4 = run_lake_query(conn, sql4, "region_breakdown", log)
        for row in rows4:
            log.detail(f"  {row[0]:2s} | {row[1]:4d} unique content | {row[2]:5d} events | {row[3]:6.0f}h")
        run.end_stage("region_breakdown", output_rows=len(rows4))

        # ── Summary ──
        conn.close()
        run.print_summary(log)
        log.info("Lake queries complete. Ready for Module 08: Star Schema Warehouse.")


if __name__ == "__main__":
    main()

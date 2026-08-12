"""Module 08 Exercise: Star Schema Warehouse — DuckDB as "Redshift".

Builds a dimensional model and loads it from the curated Parquet lake.
Demonstrates surrogate keys, fact grain enforcement, and warehouse queries.

Usage:
    uv run python -m etl_playground.m08_warehouse.exercise
"""

from pathlib import Path

import duckdb

from etl_playground.m08_warehouse.etl_load import (
    load_dimensions,
    load_facts,
    verify_warehouse,
)
from etl_playground.shared.logging import etl_context


def main() -> None:
    with etl_context("M08_Warehouse") as (log, run):
        log.header("ETL Playground — Star Schema Warehouse (DuckDB ≈ Redshift)")

        # Connect to warehouse database
        db_path = Path("data/warehouse.duckdb")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))
        conn.execute("SET enable_progress_bar = false")

        # ── Create Schema ──
        log.stage("Creating Star Schema DDL")
        schema_file = Path(__file__).parent / "schema.sql"
        schema_sql = schema_file.read_text()
        conn.execute(schema_sql)
        log.success("Star schema created: 4 dims + 2 facts")

        # ── Load Dimensions ──
        log.stage("Loading Dimension Tables")
        run.start_stage("load_dimensions")
        dim_counts = load_dimensions(conn, log)
        run.end_stage("load_dimensions", metrics=dim_counts)

        # ── Load Facts ──
        log.stage("Loading Fact Tables (ELT from Parquet lake)")
        run.start_stage("load_facts")
        fact_counts = load_facts(conn, log)
        run.end_stage("load_facts", metrics=fact_counts)

        # ── Verify ──
        verify_warehouse(conn, log)

        # ── Sample Warehouse Query ──
        log.stage("Sample Warehouse Query: Top Content by Region")
        result = conn.execute("""
            SELECT
                dr.region_name,
                dc.title,
                fv.watch_hours,
                fv.views,
                fv.completed_views
            FROM fact_viewing fv
            JOIN dim_content dc ON fv.content_key = dc.content_key
            JOIN dim_region dr ON fv.region_key = dr.region_key
            WHERE fv.watch_hours > 0
            ORDER BY fv.watch_hours DESC
            LIMIT 10
        """).fetchall()

        for row in result:
            log.detail(f"  {row[0]:20s} | {str(row[1])[:40]:40s} | {row[2]:8.1f}h | {row[3]:5d} views | {row[4]:4d} completed")

        # ── Summary ──
        log.header("Warehouse Summary")
        log.table("Star Schema Tables", [
            ("dim_date", f"{dim_counts.get('dim_date', 0):,} rows"),
            ("dim_content", f"{dim_counts.get('dim_content', 0):,} rows"),
            ("dim_region", f"{dim_counts.get('dim_region', 0):,} rows"),
            ("dim_device", f"{dim_counts.get('dim_device', 0):,} rows"),
            ("fact_viewing", f"{fact_counts.get('fact_viewing', 0):,} rows"),
        ])
        log.info(f"Warehouse database: {db_path}")

        conn.close()
        run.print_summary(log)
        log.info("Warehouse ready. Ready for Module 09: Streamlit Dashboard.")


if __name__ == "__main__":
    main()

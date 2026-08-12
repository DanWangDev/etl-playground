"""Analytical queries on the Parquet data lake (DuckDB as Athena equivalent).

These queries run directly against Parquet files in data/curated/ — no warehouse
loading required. DuckDB handles predicate pushdown, partition pruning, and
column pruning automatically.
"""

import time
from pathlib import Path

import duckdb


def run_lake_query(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    label: str = "query",
    log: "ETLLogger | None" = None,
) -> tuple[list, dict]:
    """Run a lake query and capture execution metrics.

    Args:
        conn: DuckDB connection.
        sql: SQL query (can reference parquet globs directly).
        label: Human-readable query name for logging.
        log: Optional logger.

    Returns:
        (result_rows, metrics_dict) with timing and scan info.
    """

    if log:
        log.info(f"Running lake query: {label}")

    # Get the query plan first to understand what DuckDB will do
    plan = conn.execute(f"EXPLAIN {sql}").fetchall()
    plan_text = "\n".join(str(row[0]) for row in plan)

    # Execute with timing
    t0 = time.perf_counter()
    result = conn.execute(sql).fetchall()
    elapsed = time.perf_counter() - t0

    metrics = {
        "label": label,
        "query_time_s": round(elapsed, 3),
        "row_count": len(result),
    }

    # Try to get scan metrics from EXPLAIN ANALYZE
    try:
        analyze = conn.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        analyze_text = "\n".join(str(row[0]) for row in analyze)
        metrics["explain_plan"] = plan_text[:300]
        metrics["explain_analyze"] = analyze_text[:300]
    except Exception:
        pass

    if log:
        log.detail(f"  {label}: {len(result):,} rows in {elapsed:.3f}s")

    return result, metrics


# ── Analytical Query Library ──

TOP_CONTENT_UK_SQL = """
SELECT
    content_id,
    title,
    SUM(watch_minutes) / 60.0 AS watch_hours,
    COUNT(*) AS view_count,
    ROUND(AVG(completion_rate), 2) AS avg_completion
FROM '{parquet_path}/**/*.parquet'
WHERE region = 'UK'
  AND event_date BETWEEN '2026-08-01' AND '2026-08-07'
GROUP BY content_id, title
ORDER BY watch_hours DESC
LIMIT 20
"""

WEEKLY_TREND_SQL = """
SELECT
    event_date,
    region,
    COUNT(*) AS daily_views,
    ROUND(SUM(watch_minutes) / 60.0, 1) AS watch_hours
FROM '{parquet_path}/**/*.parquet'
WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07'
GROUP BY event_date, region
ORDER BY event_date, region
"""

COMPLETION_BY_DEVICE_SQL = """
SELECT
    device_type,
    COUNT(*) AS total_events,
    ROUND(AVG(completion_rate), 2) AS avg_completion_rate,
    SUM(CASE WHEN completion_rate >= 0.95 THEN 1 ELSE 0 END) AS completed_views
FROM '{parquet_path}/**/*.parquet'
WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07'
GROUP BY device_type
ORDER BY total_events DESC
"""

REGION_BREAKDOWN_SQL = """
SELECT
    region,
    COUNT(DISTINCT content_id) AS unique_content,
    COUNT(*) AS total_events,
    ROUND(SUM(watch_minutes) / 60.0, 1) AS total_watch_hours
FROM '{parquet_path}/**/*.parquet'
WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07'
GROUP BY region
ORDER BY total_watch_hours DESC
"""

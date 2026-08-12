"""DuckDB connection factory.

DuckDB serves a dual role in this project:
1. Athena equivalent — direct Parquet lake queries with scan metrics
2. Redshift equivalent — star schema warehouse with dimensional modeling
"""

import duckdb

from etl_playground.shared.config import get_settings


def create_duckdb_connection(
    read_only: bool = False, warehouse: bool = False
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection.

    Args:
        read_only: Open in read-only mode (for lake queries).
        warehouse: Connect to the warehouse database file.

    Returns:
        A DuckDB connection with sensible defaults.
    """
    settings = get_settings()

    if warehouse or not read_only:
        db_path = str(settings.warehouse_db_path)
        settings.warehouse_db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(db_path, read_only=read_only)
    else:
        conn = duckdb.connect(":memory:")

    # Enable useful extensions/features
    conn.execute("SET enable_progress_bar = false")  # We use our own progress bars
    conn.execute("SET enable_object_cache = true")
    conn.execute("SET threads = 4")

    return conn


def query_parquet(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    explain: bool = False,
) -> tuple[list, dict | None]:
    """Run a query against Parquet files and optionally capture scan metrics.

    Args:
        conn: DuckDB connection.
        sql: SQL query (FROM paths can use globs like 'data/curated/**/*.parquet').
        explain: If True, collect EXPLAIN ANALYZE metrics.

    Returns:
        Tuple of (result_rows, scan_metrics_dict_or_None).
    """
    metrics = None

    if explain:
        explain_sql = f"EXPLAIN ANALYZE {sql}"
        result = conn.execute(explain_sql).fetchall()
        # Parse the explain output for key metrics
        explain_text = "\n".join(str(row[0]) for row in result)
        metrics = _parse_duckdb_explain(explain_text)

    result = conn.execute(sql).fetchall()
    return result, metrics


def _parse_duckdb_explain(explain_text: str) -> dict:
    """Parse DuckDB EXPLAIN ANALYZE output for key metrics."""
    import re

    metrics: dict = {"explain_raw": explain_text}

    # Extract bytes scanned
    scan_match = re.search(
        r"(\d+\.?\d*)\s*(GB|MB|KB|bytes)", explain_text, re.IGNORECASE
    )
    if scan_match:
        metrics["bytes_scanned_str"] = f"{scan_match.group(1)} {scan_match.group(2)}"

    # Extract timing
    time_match = re.search(r"Total Time:\s*([\d.]+)s", explain_text)
    if time_match:
        metrics["total_time_s"] = float(time_match.group(1))

    return metrics

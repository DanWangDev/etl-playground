"""CSV vs Partitioned Parquet benchmark.

Runs the same analytical query against:
1. Unpartitioned CSV (baseline) — full scan, no pruning
2. Partitioned Parquet (optimized) — partition + column pruning

Uses DuckDB for consistent measurement (same engine, different file formats).
"""

import time
from pathlib import Path

import duckdb

BENCHMARK_SQL = """
SELECT
    region,
    content_id,
    SUM(watch_minutes) / 60.0 AS watch_hours
FROM '{source}'
WHERE {date_filter}
  AND region = 'UK'
GROUP BY region, content_id
ORDER BY watch_hours DESC
LIMIT 20
"""

CSV_DATE_FILTER = (
    "CAST(event_timestamp AS DATE) BETWEEN '2026-08-01' AND '2026-08-07'"
)
PARQUET_DATE_FILTER = (
    "event_date BETWEEN '2026-08-01' AND '2026-08-07'"
)


def run_csv_baseline(csv_path: str, log: "object") -> dict:
    """Run benchmark query against unpartitioned CSV files."""

    conn = duckdb.connect(":memory:")
    conn.execute("SET enable_progress_bar = false")

    log.info("Running CSV baseline (unpartitioned, full scan)...")

    sql = BENCHMARK_SQL.format(
        source=str(Path(csv_path) / "*.csv"),
        date_filter=CSV_DATE_FILTER,
    )

    t0 = time.perf_counter()
    explain = conn.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    elapsed = time.perf_counter() - t0

    explain_text = "\n".join(str(row[0]) for row in explain)

    # Extract bytes scanned from EXPLAIN
    import re
    bytes_match = re.search(r'(\d+\.?\d*)\s*(MB|GB|KB)', explain_text)
    bytes_scanned = bytes_match.group(0) if bytes_match else "unknown"

    result = {
        "format": "CSV (unpartitioned)",
        "query_time_s": round(elapsed, 2),
        "bytes_scanned": bytes_scanned,
        "explain_summary": explain_text[:500],
    }

    log.detail(f"CSV: {elapsed:.2f}s, scanned: {bytes_scanned}")
    conn.close()
    return result


def run_parquet_optimized(parquet_path: str, log: "object") -> dict:
    """Run benchmark query against partitioned Parquet files."""

    conn = duckdb.connect(":memory:")
    conn.execute("SET enable_progress_bar = false")

    log.info("Running Parquet optimized (partitioned, column pruning)...")

    # Use glob to include all partition subdirectories
    sql = BENCHMARK_SQL.format(
        source=str(Path(parquet_path) / "**" / "*.parquet"),
        date_filter=PARQUET_DATE_FILTER,
    )

    t0 = time.perf_counter()
    explain = conn.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    elapsed = time.perf_counter() - t0

    explain_text = "\n".join(str(row[0]) for row in explain)

    import re
    bytes_match = re.search(r'(\d+\.?\d*)\s*(MB|GB|KB)', explain_text)
    bytes_scanned = bytes_match.group(0) if bytes_match else "unknown"

    result = {
        "format": "Parquet (partitioned by event_date + region)",
        "query_time_s": round(elapsed, 2),
        "bytes_scanned": bytes_scanned,
        "explain_summary": explain_text[:500],
    }

    log.detail(f"Parquet: {elapsed:.2f}s, scanned: {bytes_scanned}")
    conn.close()
    return result


def compare_results(csv_result: dict, parquet_result: dict, log: "object") -> None:
    """Print a comparison table of CSV vs Parquet benchmark results."""

    csv_time = csv_result["query_time_s"]
    pq_time = parquet_result["query_time_s"]

    speedup = csv_time / pq_time if pq_time > 0 else 0

    log.header("Benchmark Results")
    log.table_triple(
        "CSV (Unpartitioned) vs Parquet (Partitioned)",
        ("Metric", "CSV Baseline", "Parquet Optimized"),
        [
            ("Format", csv_result["format"], parquet_result["format"]),
            ("Query time", f"{csv_time:.2f}s", f"{pq_time:.2f}s"),
            ("Data scanned", csv_result["bytes_scanned"], parquet_result["bytes_scanned"]),
            ("Speedup", "—", f"{speedup:.1f}×"),
        ],
    )

    log.section("Why is Parquet faster?")
    log.info("1. PARTITION PRUNING: Only date+region partitions matching the filter are read")
    log.info("2. COLUMN PRUNING: Only region, content_id, watch_minutes columns are read")
    log.info("3. COMPRESSION: Parquet Snappy compression reduces I/O ~3-4× vs uncompressed CSV")
    log.info("4. PREDICATE PUSHDOWN: Filters pushed to the storage layer, reducing rows early")

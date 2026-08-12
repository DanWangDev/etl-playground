"""Module 05 Exercise: Performance Benchmark.

Compares analytical query performance: unpartitioned CSV vs partitioned Parquet.

Usage:
    uv run python -m etl_playground.m05_benchmark.exercise
"""

import argparse

from etl_playground.m05_benchmark.benchmark import (
    compare_results,
    run_csv_baseline,
    run_parquet_optimized,
)
from etl_playground.shared.logging import etl_context


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CSV vs Parquet benchmark")
    parser.add_argument(
        "--csv-path", type=str, default="data/curated/viewing_events_csv"
    )
    parser.add_argument(
        "--parquet-path", type=str, default="data/curated/viewing_events_enriched"
    )
    args = parser.parse_args()

    with etl_context("M05_Benchmark") as (log, run):
        log.header("ETL Playground — Performance Benchmark")
        log.info("CSV (unpartitioned) vs Parquet (partitioned by event_date + region)")
        log.info(
            "Query: Top 20 content by watch hours, UK region, 2026-08-01 to 2026-08-07"
        )

        # ── Run benchmarks ──
        log.stage("Baseline: Unpartitioned CSV")
        run.start_stage("csv_baseline")
        csv_result = run_csv_baseline(args.csv_path, log)
        run.end_stage("csv_baseline", metrics=csv_result)

        log.stage("Optimized: Partitioned Parquet")
        run.start_stage("parquet_optimized")
        parquet_result = run_parquet_optimized(args.parquet_path, log)
        run.end_stage("parquet_optimized", metrics=parquet_result)

        # ── Compare ──
        compare_results(csv_result, parquet_result, log)

        # ── Save benchmark to docs ──
        log.info(
            "Benchmark results can be saved to docs/benchmark-results.md for reference."
        )

        run.print_summary(log)


if __name__ == "__main__":
    main()

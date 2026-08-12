"""Module 04 Exercise: Curated Output — Partitioned Parquet.

Writes the transformed data as partitioned Parquet and inspects the partition structure.

Usage:
    uv run python -m etl_playground.m04_curated_output.exercise
"""

import argparse
from datetime import date

from etl_playground.m04_curated_output.writer import (
    list_partitions,
    write_partitioned_parquet,
)
from etl_playground.shared.logging import etl_context
from etl_playground.shared.spark import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Write curated Parquet output")
    parser.add_argument("--ingest-date", type=str, default=None)
    args = parser.parse_args()

    with etl_context("M04_Curated_Output") as (log, run):
        log.header("ETL Playground — Curated Output")

        spark = create_spark_session("m04-curated-output")
        ingest_date = (
            date.today()
            if args.ingest_date is None
            else date.fromisoformat(args.ingest_date)
        )

        # Read enriched detail from M03
        enriched_path = f"data/curated/viewing_events_enriched/ingest_date={ingest_date.isoformat()}"
        log.info(f"Reading enriched events from: {enriched_path}")

        try:
            df = spark.read.parquet(enriched_path)
        except Exception:
            log.warn(
                f"No enriched data found at {enriched_path}. "
                f"Run Module 03 first to generate it."
            )
            log.info("Falling back to raw data for demonstration...")
            from etl_playground.m02_spark_foundation.reader import read_viewing_events

            df = read_viewing_events(spark, ingest_date)

        log.info(f"Loaded: {df.count():,} rows")

        # Show available columns
        log.detail(f"Columns: {', '.join(df.columns)}")

        # ── Write partitioned Parquet ──
        log.stage("Writing Partitioned Parquet")
        run.start_stage("write_parquet")

        output_path = (
            f"data/curated/viewing_events/ingest_date={ingest_date.isoformat()}"
        )
        write_partitioned_parquet(
            df,
            output_path,
            partition_keys=["event_date", "region"],
            log=log,
        )
        run.end_stage("write_parquet", output_rows=df.count())

        # ── Inspect partitions ──
        log.stage("Inspecting Partition Structure")
        partitions = list_partitions(output_path, log)

        # ── Explain partitioning choices ──
        log.section("Partitioning Rationale")
        log.table(
            "Partition Key Design",
            [
                (
                    "event_date",
                    "Aligns with time-range queries (WHERE event_date BETWEEN ...)",
                ),
                ("region", "Aligns with regional filters (WHERE region = 'UK')"),
                (
                    "Anti-pattern: customer_id",
                    "Too many unique values → millions of tiny files",
                ),
                (
                    "Anti-pattern: content_id",
                    "High cardinality + uneven distribution (hot content)",
                ),
            ],
        )
        log.info(
            "event_date + region = low cardinality, query-aligned, manageable partition count"
        )

        run.print_summary(log)
        log.info(f"Partitioned output ready. {len(partitions)} Parquet files written.")
        log.info("Ready for Module 05: Performance Benchmark.")

        spark.stop()


if __name__ == "__main__":
    main()

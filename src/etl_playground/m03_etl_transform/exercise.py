"""Module 03 Exercise: Full ETL Transform Pipeline.

Runs the complete transform: normalize → dedup → enrich → derive → aggregate.

Usage:
    uv run python -m etl_playground.m03_etl_transform.exercise
"""

import argparse
import time
from datetime import date

from etl_playground.m02_spark_foundation.reader import (
    read_content_launch,
    read_content_metadata,
    read_viewing_events,
)
from etl_playground.m03_etl_transform.aggregator import aggregate_daily
from etl_playground.m03_etl_transform.dedup import deduplicate_by_event_id
from etl_playground.m03_etl_transform.derive import derive_fields
from etl_playground.m03_etl_transform.enrich import (
    enrich_with_launch,
    enrich_with_metadata,
)
from etl_playground.m03_etl_transform.normalize import normalize_timestamps
from etl_playground.shared.logging import etl_context
from etl_playground.shared.spark import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full ETL transform pipeline")
    parser.add_argument("--ingest-date", type=str, default=None)
    args = parser.parse_args()

    with etl_context("M03_Transform") as (log, run):
        log.header("ETL Playground — Full Transform")

        spark = create_spark_session("m03-etl-transform")
        ingest_date = (
            date.today()
            if args.ingest_date is None
            else date.fromisoformat(args.ingest_date)
        )

        # ── Read raw data ──
        log.stage("Stage 0/6: Read Raw Data")
        t0 = time.monotonic()
        df_raw = read_viewing_events(spark, ingest_date)
        df_metadata = read_content_metadata(spark)
        df_launch = read_content_launch(spark)
        log.info(
            f"Viewing events: {df_raw.count():,} rows, "
            f"Content metadata: {df_metadata.count():,}, "
            f"Content launch: {df_launch.count():,}"
        )

        # ── Stage 1: Normalize ──
        log.stage("Stage 1/6: Normalize Timestamps")
        run.start_stage("normalize")
        df = normalize_timestamps(df_raw, log)
        run.end_stage("normalize", input_rows=df_raw.count(), output_rows=df.count())

        # ── Stage 2: Deduplicate ──
        log.stage("Stage 2/6: Deduplicate by event_id")
        run.start_stage("dedup")
        df, duplicates = deduplicate_by_event_id(df, log)
        run.end_stage("dedup", output_rows=df.count(), duplicates=duplicates.count())

        # ── Stage 3: Enrich with metadata ──
        log.stage("Stage 3/6: Enrich with Content Metadata (Broadcast Join)")
        run.start_stage("enrich_metadata")
        df = enrich_with_metadata(df, df_metadata, log)
        run.end_stage("enrich_metadata", output_rows=df.count())

        # ── Stage 4: Enrich with launch data ──
        log.stage("Stage 4/6: Enrich with Content Launch (Sort-Merge Join)")
        run.start_stage("enrich_launch")
        df = enrich_with_launch(df, df_launch, log)
        run.end_stage("enrich_launch", output_rows=df.count())

        # ── Stage 5: Derive fields ──
        log.stage("Stage 5/6: Derive Calculated Fields")
        run.start_stage("derive")
        df = derive_fields(df, log)
        run.end_stage("derive", output_rows=df.count())

        # ── Stage 6: Aggregate ──
        log.stage("Stage 6/6: Aggregate to Daily Grain")
        run.start_stage("aggregate")
        df_daily = aggregate_daily(df, log)
        run.end_stage("aggregate", output_rows=df_daily.count())

        # ── Write intermediate output for next modules ──
        daily_path = (
            f"data/curated/daily_aggregates/ingest_date={ingest_date.isoformat()}"
        )
        df_daily.write.mode("overwrite").parquet(daily_path)
        log.success(f"Daily aggregates written: {daily_path}")

        # Write enriched detail for curated output module
        detail_path = f"data/curated/viewing_events_enriched/ingest_date={ingest_date.isoformat()}"
        df.write.mode("overwrite").parquet(detail_path)
        log.success(f"Enriched detail written: {detail_path}")

        total_time = time.monotonic() - t0
        run.print_summary(log)
        log.info(f"Ready for Module 04: Curated Output. ({total_time:.1f}s)")

        spark.stop()


if __name__ == "__main__":
    main()

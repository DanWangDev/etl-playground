"""Module 02 Exercise: PySpark Foundation.

Demonstrates:
1. Schema definition and enforcement
2. Lazy evaluation (transformations vs actions)
3. DAG / execution plan inspection
4. Field validation with dead-letter capture
5. Unpartitioned Parquet output (baseline for M05 benchmark)

Usage:
    uv run python -m etl_playground.m02_spark_foundation.exercise
"""

import argparse
import time
from datetime import date

from etl_playground.m02_spark_foundation.reader import read_viewing_events
from etl_playground.m02_spark_foundation.validators import validate_all
from etl_playground.shared.logging import get_logger
from etl_playground.shared.paths import (
    curated_csv_unpartitioned_dir,
    curated_viewing_events_dir,
    rejected_dir,
)
from etl_playground.shared.spark import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run PySpark foundation exercises"
    )
    parser.add_argument(
        "--ingest-date", type=str, default=None,
        help="Ingestion date to read (default: today)"
    )
    args = parser.parse_args()

    log = get_logger("M02_Spark_Foundation")
    log.header("ETL Playground — Spark Foundation")

    # ── Initialize Spark ──
    spark = create_spark_session("m02-spark-foundation")

    log.key_value("Spark master", spark.sparkContext.master)
    log.key_value("Driver memory", spark.sparkContext.getConf().get("spark.driver.memory", "default"))
    log.key_value("Spark UI", spark.sparkContext.uiWebUrl or "not available")

    ingest_date = date.today() if args.ingest_date is None else date.fromisoformat(args.ingest_date)

    # ── Step 1: Define Schemas ──
    log.stage("Step 1/6: Define Schemas")
    log.success("viewing_events: 10 columns (event_id, customer_id, ..., subscription_type)")
    log.success("content_metadata: 6 columns")
    log.success("content_launch: 5 columns")

    # ── Step 2: Read Raw Data (lazy) ──
    log.stage("Step 2/6: Read Raw Data (LAZY — no data read yet)")
    t0 = time.monotonic()

    df_raw = read_viewing_events(spark, ingest_date)
    log.info(f"DataFrame created from data/raw/viewing_events/ingest_date={ingest_date.isoformat()}/")
    log.spark(f"Schema: {', '.join(f.name for f in df_raw.schema.fields)}")

    # Demonstrate lazy eval: show the logical plan without executing
    log.spark("NOTE: DataFrame is LAZY — transformations build a DAG, actions trigger execution")
    log.detail("Logical plan (truncated):")
    for line in str(df_raw._jdf.queryExecution().logical()).split("\n")[:8]:
        if line.strip():
            log.detail(f"  {line.strip()[:120]}")

    # ── Step 3: Inspect (lazy transformation) ──
    log.stage("Step 3/6: Inspect DataFrame (still lazy)")
    log.info("df_raw.printSchema():")
    df_raw.printSchema()

    # ── Step 4: Validate ──
    log.stage("Step 4/6: Validate & Filter (ACTION — triggers first Spark job)")
    df_valid, df_rejected = validate_all(df_raw, log)

    # Write rejected records
    rejected_count = df_rejected.count()
    if rejected_count > 0:
        t1 = time.monotonic()
        rej_path = rejected_dir(ingest_date)
        df_rejected.coalesce(1).write.mode("overwrite").csv(
            str(rej_path), header=True
        )
        log.info(f"Rejected records written to: {str(rej_path)} "
                 f"({rejected_count:,} rows, {time.monotonic() - t1:.1f}s)")

    # ── Step 5: Execution Plan ──
    log.stage("Step 5/6: Execution Plan (Physical Plan)")
    log.info("df_valid.explain(extended=True) output:")
    for line in df_valid._jdf.queryExecution().executedPlan().toString().split("\n")[:15]:
        if line.strip():
            log.detail(f"  {line.strip()[:130]}")

    # ── Step 6: Write Unpartitioned CSV + Parquet (baseline for benchmark) ──
    log.stage("Step 6/6: Write Output (ACTION — triggers final Spark jobs)")

    # CSV baseline (for M05 benchmark comparison)
    t2 = time.monotonic()
    csv_path = curated_csv_unpartitioned_dir()
    log.info(f"Writing unpartitioned CSV baseline to: {csv_path}")
    df_valid.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_path))
    csv_time = time.monotonic() - t2
    log.spark(f"CSV baseline written in {csv_time:.1f}s")

    # Unpartitioned Parquet (quick sanity)
    t3 = time.monotonic()
    parquet_path = curated_viewing_events_dir()
    log.info(f"Writing unpartitioned Parquet to: {parquet_path}")
    df_valid.write.mode("overwrite").parquet(str(parquet_path))
    parquet_time = time.monotonic() - t3
    log.spark(f"Parquet written in {parquet_time:.1f}s")

    # ── Summary ──
    total_time = time.monotonic() - t0
    log.header("Module 02 Complete")
    log.table("Results", [
        ("Valid rows", f"{df_valid.count():,}"),
        ("Rejected rows", f"{rejected_count:,}"),
        ("CSV baseline path", str(csv_path)),
        ("Parquet path", str(parquet_path)),
        ("Total time", f"{total_time:.1f}s"),
        ("Spark UI", spark.sparkContext.uiWebUrl or "not available"),
    ])
    log.info("Ready for Module 03: ETL Transform.")
    log.info("NOTE: Leave Spark UI open at " + (spark.sparkContext.uiWebUrl or "N/A") +
             " to inspect job DAG, shuffle, and task metrics.")

    spark.stop()


if __name__ == "__main__":
    main()

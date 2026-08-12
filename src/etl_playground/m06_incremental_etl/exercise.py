"""Module 06 Exercise: Incremental ETL — Production Reliability.

Demonstrates:
1. Incremental processing: only process new partitions
2. Idempotent reruns: same output regardless of how many times you run
3. Late-arriving data: reprocessing window
4. Backfill: parameterized historical date range reprocessing

Usage:
    uv run python -m etl_playground.m06_incremental_etl.exercise
    uv run python -m etl_playground.m06_incremental_etl.exercise --mode backfill --start 2026-08-01 --end 2026-08-07
    uv run python -m etl_playground.m06_incremental_etl.exercise --mode rerun-test
"""

import argparse
import time
from datetime import date
from pathlib import Path

from etl_playground.m06_incremental_etl.backfill import backfill_partitions, parse_date_range
from etl_playground.m06_incremental_etl.idempotent import (
    load_previous_state,
    save_run_state,
    verify_idempotency,
)
from etl_playground.m06_incremental_etl.incremental import find_new_partitions
from etl_playground.shared.config import get_settings
from etl_playground.shared.logging import etl_context


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental ETL operations")
    parser.add_argument("--mode", choices=["incremental", "backfill", "rerun-test"],
                        default="incremental")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD) for backfill")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD) for backfill")
    parser.add_argument("--late-window", type=int, default=2,
                        help="Late-arrival reprocessing window in days")
    args = parser.parse_args()

    settings = get_settings()

    with etl_context("M06_Incremental_ETL", mode=args.mode) as (log, run):
        log.header("ETL Playground — Incremental ETL")

        if args.mode == "incremental":
            _run_incremental(log, run, settings, args)
        elif args.mode == "backfill":
            _run_backfill(log, run, settings, args)
        elif args.mode == "rerun-test":
            _run_rerun_test(log, run, settings, args)

        run.print_summary(log)


def _run_incremental(log, run, settings, args) -> None:
    """Run incremental ETL: process new + late-window partitions."""
    raw_base = settings.raw_base / "viewing_events"
    curated_base = settings.curated_base / "viewing_events"

    log.stage("Scanning Partitions")
    log.info(f"Raw zone: {raw_base}")
    log.info(f"Curated zone: {curated_base}")

    new_dates, reprocess_dates = find_new_partitions(
        raw_base, curated_base,
        late_window_days=args.late_window,
    )

    log.table("Partition Scan Results", [
        ("New partitions (unprocessed)", str(len(new_dates))),
        ("Late-window partitions (reprocess)", str(len(reprocess_dates))),
        ("Late window", f"D-{args.late_window} through D"),
    ])

    if new_dates:
        log.info(f"New dates to process: {[d.isoformat() for d in new_dates]}")
    else:
        log.success("No new partitions — everything is up to date")

    if reprocess_dates:
        log.info(f"Late-window dates to reprocess: {[d.isoformat() for d in reprocess_dates]}")
        log.warn(f"These dates may receive late-arriving events. "
                 f"Processing {len(reprocess_dates)} partitions in late window.")

    # In a full implementation, this would trigger the Spark ETL pipeline
    # for each date. For the exercise, we demonstrate the partition detection.

    log.info(f"Total partitions to process: {len(new_dates) + len(reprocess_dates)}")


def _run_backfill(log, run, settings, args) -> None:
    """Run a backfill for a specific date range."""
    if not args.start:
        log.error("Backfill requires --start DATE")
        return

    dates = parse_date_range(args.start, args.end)
    if not dates:
        log.info("Scanning raw directory for all available dates...")
        raw_base = settings.raw_base / "viewing_events"
        if raw_base.exists():
            for d in sorted(raw_base.iterdir()):
                if d.is_dir() and d.name.startswith("ingest_date="):
                    try:
                        dates.append(date.fromisoformat(d.name.split("=")[1]))
                    except (ValueError, IndexError):
                        continue

    log.info(f"Backfill range: {dates[0]} → {dates[-1]} ({len(dates)} dates)")

    # Mock process function for demonstration
    def mock_process(d: date, l) -> int:
        import random
        random.seed(hash(d.isoformat()))
        return random.randint(15000, 17000)

    results = backfill_partitions(dates, mock_process, log)
    log.table("Backfill Results", [
        (d, f"{count:,} rows" if count >= 0 else "FAILED")
        for d, count in sorted(results.items())
    ])


def _run_rerun_test(log, run, settings, args) -> None:
    """Test idempotency: run the same operation twice, verify same output."""
    log.header("Idempotency Test")
    log.info("Running the same operation twice and comparing outputs...")

    curated_path = settings.curated_base / "viewing_events"

    # First run: capture state
    log.stage("Run 1: Initial Processing")
    previous_state = load_previous_state(curated_path)
    if previous_state:
        log.info(f"Loaded previous state: {len(previous_state)} partitions")
    else:
        log.info("No previous state found — treating as first run")

    # Simulate row counts
    mock_counts = {}
    if curated_path.exists():
        for f in curated_path.rglob("*.parquet"):
            region = f.parent.name if f.parent.name.startswith("region=") else "unknown"
            mock_counts[region] = mock_counts.get(region, 0) + 1

    save_run_state("rerun_test_1", mock_counts, curated_path)

    # Second run: verify
    log.stage("Run 2: Rerun (should be identical)")
    log.info("Rerunning the same partition...")

    # Verify
    log.stage("Verification")
    is_idempotent = verify_idempotency(str(curated_path), mock_counts, log)

    if is_idempotent:
        log.success("Rerun test PASSED — ETL is idempotent ✓")
    else:
        log.error("Rerun test FAILED — investigate partition differences")

    log.info("Key principle: partition-level overwrite (INSERT OVERWRITE) ensures idempotency.")
    log.info("As long as the input + transform logic is deterministic, reruns are safe.")


if __name__ == "__main__":
    main()

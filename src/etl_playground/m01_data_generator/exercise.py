"""Module 01 Exercise: Generate synthetic content analytics data.

Usage:
    uv run python -m etl_playground.m01_data_generator.exercise
    uv run python -m etl_playground.m01_data_generator.exercise --scale 5000000

Environment variables (from .env):
    DATA_SCALE — number of viewing events (default: 500000)
    RANDOM_SEED — deterministic seed (default: 42)
"""

import argparse
import os
import time
from datetime import date

from etl_playground.m01_data_generator.generators import (
    generate_content_launch,
    generate_content_metadata,
    generate_viewing_events,
    write_content_launch_csv,
    write_content_metadata_csv,
    write_viewing_events_csv,
)
from etl_playground.shared.config import get_settings
from etl_playground.shared.logging import get_logger, progress_bar
from etl_playground.shared.paths import ensure_all_dirs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic content analytics data"
    )
    parser.add_argument(
        "--scale", type=int, default=None,
        help="Number of viewing events (overrides DATA_SCALE env var)"
    )
    args = parser.parse_args()

    # Override scale if provided
    if args.scale:
        os.environ["DATA_SCALE"] = str(args.scale)

    settings = get_settings()
    log = get_logger("M01_Data_Generator")

    # ── Header ──
    log.header("ETL Playground — Data Generator")

    log.key_value("Scale", f"{settings.data_scale:,} records")
    log.key_value("Random seed", str(settings.random_seed))
    log.key_value("Content items", str(settings.num_content_items))
    log.key_value("Regions", str(settings.region_list))
    log.key_value("Target", "data/raw/")

    ensure_all_dirs()
    t0 = time.monotonic()

    # ── Step 1: Generate content_metadata ──
    log.stage("Stage 1/3: Content Metadata")
    content_items = generate_content_metadata(log)
    write_content_metadata_csv(content_items)
    log.detail(f"Written: data/raw/content_metadata/metadata.csv "
               f"({len(content_items):,} rows)")

    # ── Step 2: Generate content_launch ──
    log.stage("Stage 2/3: Content Launch Records")
    launch_items = generate_content_launch(log, content_items)
    write_content_launch_csv(launch_items)
    log.detail(f"Written: data/raw/content_launch/launch.csv "
               f"({len(launch_items):,} rows)")

    # ── Step 3: Generate viewing_events ──
    log.stage("Stage 3/3: Viewing Events")
    ingest_date = date.today()
    events = generate_viewing_events(log, content_items, launch_items)
    write_viewing_events_csv(events, ingest_date)

    # File size
    events_path = (
        get_settings().raw_base / "viewing_events"
        / f"ingest_date={ingest_date.isoformat()}" / "events.csv"
    )
    size_mb = events_path.stat().st_size / (1024 * 1024) if events_path.exists() else 0

    log.detail(f"Written: data/raw/viewing_events/ingest_date={ingest_date.isoformat()}/"
               f"events.csv ({size_mb:.1f} MB)")

    # ── Summary ──
    elapsed = time.monotonic() - t0
    log.header("Generation Complete")
    log.table("Output Summary", [
        ("Content metadata", f"{len(content_items):,} items"),
        ("Content launches", f"{len(launch_items):,} records ({len(settings.region_list)} regions × {len(content_items)} content)"),
        ("Viewing events", f"{len(events):,} events ({size_mb:.1f} MB)"),
        ("Date range", f"{settings.start_date} → {ingest_date.isoformat()}"),
        ("Total time", f"{elapsed:.1f}s"),
    ])
    log.info("Ready for Module 02: Spark Foundation.")


if __name__ == "__main__":
    main()

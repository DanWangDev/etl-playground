"""Path resolution utilities for data zones.

Mirrors S3 prefix organization: raw (immutable), curated (partitioned Parquet), rejected (dead-letter).
"""

from datetime import date
from pathlib import Path

from etl_playground.shared.config import get_settings


def raw_viewing_events_dir(ingest_date: date | None = None) -> Path:
    """data/raw/viewing_events/ingest_date=YYYY-MM-DD/"""
    settings = get_settings()
    base = settings.raw_base / "viewing_events"
    if ingest_date:
        base = base / f"ingest_date={ingest_date.isoformat()}"
    return base


def raw_content_metadata_path() -> Path:
    """data/raw/content_metadata/metadata.csv"""
    settings = get_settings()
    path = settings.raw_base / "content_metadata" / "metadata.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def raw_content_launch_path() -> Path:
    """data/raw/content_launch/launch.csv"""
    settings = get_settings()
    path = settings.raw_base / "content_launch" / "launch.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def curated_viewing_events_dir(
    event_date: date | None = None, region: str | None = None
) -> Path:
    """data/curated/viewing_events/event_date=YYYY-MM-DD/region=XX/"""
    settings = get_settings()
    base = settings.curated_base / "viewing_events"
    if event_date:
        base = base / f"event_date={event_date.isoformat()}"
    if region:
        base = base / f"region={region}"
    return base


def curated_csv_unpartitioned_dir() -> Path:
    """data/curated/viewing_events_csv/ (for benchmark baseline)"""
    return get_settings().curated_base / "viewing_events_csv"


def curated_daily_aggregates_dir() -> Path:
    """data/curated/daily_aggregates/"""
    path = get_settings().curated_base / "daily_aggregates"
    path.mkdir(parents=True, exist_ok=True)
    return path


def rejected_dir(ingest_date: date | None = None) -> Path:
    """data/rejected/ingest_date=YYYY-MM-DD/"""
    base = get_settings().rejected_base
    if ingest_date:
        base = base / f"ingest_date={ingest_date.isoformat()}"
    base.mkdir(parents=True, exist_ok=True)
    return base


def ensure_all_dirs() -> None:
    """Ensure all data zone directories exist."""
    for dir_path in [
        get_settings().raw_base / "viewing_events",
        get_settings().raw_base / "content_metadata",
        get_settings().raw_base / "content_launch",
        get_settings().curated_base / "viewing_events",
        get_settings().curated_base / "daily_aggregates",
        get_settings().rejected_base,
        get_settings().runs_base,
        get_settings().schema_base,
    ]:
        dir_path.mkdir(parents=True, exist_ok=True)

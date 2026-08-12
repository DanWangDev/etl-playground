"""Incremental processing — detect and process only new or late-arriving partitions.

Core logic:
1. Scan existing curated partitions
2. Find new raw data partitions not yet processed
3. Process only the delta (new + late-window partitions)
"""

from datetime import date, timedelta
from pathlib import Path


def find_new_partitions(
    raw_base: Path,
    curated_base: Path,
    late_window_days: int = 2,
    today: date | None = None,
) -> tuple[list[date], list[date]]:
    """Find partitions that need processing.

    Scans the raw data directory for ingest_date=YYYY-MM-DD/ directories and
    compares with curated output directories to find unprocessed dates.

    Also identifies dates within the late-arrival window that should be reprocessed.

    Args:
        raw_base: Path to data/raw/viewing_events/
        curated_base: Path to data/curated/viewing_events/
        late_window_days: Number of days back to reprocess for late arrivals.
        today: Reference date (default: today).

    Returns:
        (new_dates, reprocess_dates) — dates to process fresh and dates to reprocess.
    """
    if today is None:
        today = date.today()

    # Find existing raw partitions
    raw_dates: set[date] = set()
    if raw_base.exists():
        for d in raw_base.iterdir():
            if d.is_dir() and d.name.startswith("ingest_date="):
                try:
                    raw_dates.add(date.fromisoformat(d.name.split("=")[1]))
                except (ValueError, IndexError):
                    continue

    # Find existing curated partitions
    curated_dates: set[date] = set()
    if curated_base.exists():
        for d in curated_base.iterdir():
            if d.is_dir() and d.name.startswith("event_date=") or d.is_dir() and d.name.startswith("ingest_date="):
                try:
                    curated_dates.add(date.fromisoformat(d.name.split("=")[1]))
                except (ValueError, IndexError):
                    continue

    # New dates: in raw but not in curated
    new_dates = sorted(raw_dates - curated_dates)

    # Late-arrival window: dates that exist in both but are within the window
    # (reprocess D-2 through D to catch late-arriving events)
    window_start = today - timedelta(days=late_window_days)
    reprocess_dates = sorted(
        d for d in curated_dates
        if d >= window_start and d <= today
    )

    return new_dates, reprocess_dates

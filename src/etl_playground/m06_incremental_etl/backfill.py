"""Backfill processing — reprocess a historical date range.

Parameterized job that reprocesses raw data for a specified date range.
Used for:
- Initial historical load
- Correcting data issues in past partitions
- Schema evolution backfills
"""

from datetime import date, timedelta


def parse_date_range(
    start: str,
    end: str | None = None,
) -> list[date]:
    """Parse a date range string into a list of dates.

    Args:
        start: Start date in YYYY-MM-DD format, or "all" for full backfill.
        end: End date (inclusive). Defaults to today.

    Returns:
        List of dates to backfill.
    """
    today = date.today()

    if start == "all":
        # Full backfill: scan all available raw partitions
        return []  # Caller should scan raw directory

    start_date = date.fromisoformat(start)
    end_date = today if end is None else date.fromisoformat(end)

    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def backfill_partitions(
    dates: list[date],
    process_fn,
    log: "object",
) -> dict[str, int]:
    """Process a list of dates through the ETL pipeline.

    Args:
        dates: List of dates to process.
        process_fn: Function that takes (date, log) and returns row count.
        log: Logger.

    Returns:
        Dict of {date.isoformat(): row_count} for each processed date.
    """

    results: dict[str, int] = {}
    total = len(dates)

    log.info(f"Backfill: {total} dates to process")
    for i, d in enumerate(dates):
        log.info(f"[{i+1}/{total}] Processing {d.isoformat()}...")
        try:
            count = process_fn(d, log)
            results[d.isoformat()] = count
            log.success(f"  {d.isoformat()}: {count:,} rows")
        except Exception as exc:
            log.error(f"  {d.isoformat()}: FAILED — {exc}")
            results[d.isoformat()] = -1

    succeeded = sum(1 for v in results.values() if v >= 0)
    log.info(f"Backfill complete: {succeeded}/{total} dates succeeded")
    return results

# M06: Incremental ETL & Reliability

## What You'll Learn

- How to process only new data instead of full table scans
- Designing idempotent pipelines that produce identical output on rerun
- Late-arriving data handling with rolling reprocessing windows
- Backfill strategies for historical data
- Schema evolution policies

## Overview

A one-shot batch job is easy. A production pipeline that runs every day without corrupting data is hard. This module covers the patterns that make ETL reliable.

```
┌──────────────────────────────────────────────────────────────────┐
│                  INCREMENTAL ETL (M06)                            │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │              DAILY INCREMENTAL RUN                        │    │
│  │                                                          │    │
│  │  1. Scan raw/ for new ingest_date=* partitions           │    │
│  │  2. Check curated/ for existing event_date=* partitions  │    │
│  │  3. Process only NEW dates (delta)                       │    │
│  │  4. Reprocess D-2 through D (late-arrival window)        │    │
│  │  5. OVERWRITE curated partition (idempotent)             │    │
│  │  6. Emit row counts, rejection counts, duration          │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │              BACKFILL (on demand)                         │    │
│  │                                                          │    │
│  │  python -m etl_playground.m06_incremental_etl.exercise   │    │
│  │      --mode backfill --start 2026-01-01 --end 2026-06-30 │    │
│  └──────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Idempotency

An operation is **idempotent** if running it once has the same effect as running it N times.

**Our strategy**: Partition-level overwrite. When we process `event_date=2026-08-12`, we write with `mode("overwrite")`. If the input data hasn't changed, the output is bit-for-bit identical.

```
Run 1: process 2026-08-12 → overwrite curated/event_date=2026-08-12/
Run 2: process 2026-08-12 → overwrite curated/event_date=2026-08-12/  (same result)
```

**What breaks idempotency**:
- `mode("append")` — duplicates rows on rerun
- Non-deterministic transforms (e.g., `current_timestamp()` in derive logic)
- Reading from a source that changes between runs

### 2. Late-Arriving Data

Events don't always arrive on time. A user's device might be offline, or a batch upload might be delayed.

**Late-arrival window**: D-2 through D. On each run, we reprocess the last 3 days of data, not just today.

```
Today is 2026-08-12:
  Process NEW:    2026-08-12 (never processed before)
  Re-process:     2026-08-11, 2026-08-10 (may have late events)
  Skip:           2026-08-09 and earlier (stable)
```

The window size is a trade-off: larger = catches more late events but costs more compute.

### 3. Backfill

Backfill processes a historical date range — useful for:
- Initial load: process a year of historical data
- Bug fix: reprocess dates affected by a code change
- Schema evolution: rewrite old partitions in a new format

```bash
uv run python -m etl_playground.m06_incremental_etl.exercise \
    --mode backfill --start 2026-01-01 --end 2026-01-31
```

### 4. Partition Detection

Instead of a full table scan, we compare directory listings:

```python
raw_dates = {d for d in listdir("data/raw/") if d.startswith("ingest_date=")}
curated_dates = {d for d in listdir("data/curated/") if d.startswith("event_date=")}
new_dates = raw_dates - curated_dates  # Only process these
```

This is O(n) in the number of partitions, not O(n) in the number of rows.

## Walkthrough

```bash
# Normal incremental run
uv run python -m etl_playground.m06_incremental_etl.exercise

# Test idempotency
uv run python -m etl_playground.m06_incremental_etl.exercise --mode rerun-test
```

## Gotchas

- **Overwrite is not atomic**: If the job fails mid-write, the partition may be corrupted. In production, write to a temp location, then atomically rename.
- **Late window vs backfill**: Late window = automatic (every run). Backfill = manual (on demand).
- **Schema evolution**: Additive changes (new column) are safe. Breaking changes (renamed column) require a backfill.

## Interview Connection

- "How do you make an ETL pipeline idempotent?"
- "How do you handle late-arriving data?"
- "What's your backfill strategy?"
- "How do you detect which partitions need processing without scanning all data?"

## Next

→ [M07: Lake Queries](../m07_lake_queries/concept.md)

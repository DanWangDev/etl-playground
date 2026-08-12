# M03: ETL Transform

## What You'll Learn

- How shuffle works and why it's the most expensive Spark operation
- Broadcast join vs sort-merge join — when to use each
- Window functions for deduplication
- Timestamp normalization and date derivation
- Data quality metrics emission

## Overview

This is the heart of the ETL pipeline. Six stages transform raw, validated events into enriched, aggregated analytical datasets.

```
┌──────────────────────────────────────────────────────────────────┐
│                     ETL TRANSFORM (M03)                           │
│                                                                  │
│  Stage 1: NORMALIZE           Stage 4: ENRICH (launch)           │
│  ┌──────────────────┐        ┌──────────────────────┐            │
│  │ timestamps → UTC │        │ sort-merge join with │            │
│  │ derive event_date│        │ content_launch       │            │
│  │ year, month, day │        │ key: content_id+region│           │
│  │ (narrow — no     │        │ (SHUFFLE: both sides │            │
│  │  shuffle)        │        │  redistributed)      │            │
│  └────────┬─────────┘        └──────────┬───────────┘            │
│           │                             │                         │
│  Stage 2: DEDUP              Stage 5: DERIVE                     │
│  ┌──────────────────┐        ┌──────────────────────┐            │
│  │ row_number() OVER │       │ launch_delay_days     │            │
│  │ PARTITION BY      │       │ watch_hours           │            │
│  │ event_id ORDER BY │       │ is_completed flag     │            │
│  │ timestamp DESC    │       │ (narrow — no shuffle) │            │
│  │ (SHUFFLE: window  │       └──────────┬───────────┘            │
│  │  redistribution)  │                  │                         │
│  └────────┬─────────┘                  │                         │
│           │                             │                         │
│  Stage 3: ENRICH (metadata)   Stage 6: AGGREGATE                 │
│  ┌──────────────────┐        ┌──────────────────────┐            │
│  │ BROADCAST JOIN    │       │ GROUP BY content_id,  │            │
│  │ with content_meta │       │ region, device_type,  │            │
│  │ (1K rows → fits   │       │ event_date            │            │
│  │  in memory)       │       │ (SHUFFLE: GROUP BY   │            │
│  │ (no shuffle!)     │       │  redistribution)      │            │
│  └──────────────────┘        └──────────────────────┘            │
└──────────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Shuffle — The Most Expensive Operation

A **shuffle** occurs when Spark must redistribute data across partitions. Data from every partition is sent to every other partition — this means network I/O, disk I/O, and serialization.

```
Before shuffle:              After shuffle (by key):
Partition 0: [A, B, C]       Partition 0: [A, A, A]
Partition 1: [A, D, E]  →    Partition 1: [B, B]
Partition 2: [B, C, A]       Partition 2: [C, C, D, E]
```

In this module, shuffle happens in:
- **Dedup**: `PARTITION BY event_id` — all rows with the same event_id must be on the same partition
- **Sort-merge join**: Both sides are redistributed by join keys
- **Aggregation**: `GROUP BY` redistributes by the grouping columns

### 2. Broadcast Join — Avoiding Shuffle

When one table is small enough to fit in executor memory (< 10 MB), Spark can **broadcast** it to every executor instead of shuffling:

```
Broadcast Join:                    Sort-Merge Join:
┌──────┐  ┌──────┐                ┌──────┐    ┌──────┐
│ Big  │  │Small │                │ Big  │    │Small │
│Table │  │Table │                │Table │    │Table │
│      │  │(1K)  │                │      │    │      │
└──┬───┘  └──┬───┘                └──┬───┘    └──┬───┘
   │         │                       │           │
   │    ┌────▼────┐                  ▼           ▼
   │    │Broadcast│              ┌─────────────────┐
   │    │to all   │              │    SHUFFLE      │
   │    │executors│              │ redistribute by │
   │    └────┬────┘              │    join key     │
   ▼         ▼                   └────────┬────────┘
┌─────────────────┐                       ▼
│  Local join on  │              ┌─────────────────┐
│  each executor  │              │  Sort + Merge   │
│  (no shuffle)   │              │  on each exec   │
└─────────────────┘              └─────────────────┘
```

In our pipeline: `content_metadata` (1,000 rows, ~0.1 MB) → perfect broadcast candidate.

### 3. Window Functions for Dedup

```sql
row_number() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC)
```

This assigns row 1 to the latest event for each event_id. We keep row 1 (latest) and send row > 1 (older duplicates) to the rejected queue.

### 4. Data Quality Metrics

Every stage emits metrics:

```
┌─────────────────────────┬──────────┐
│ Metric                  │ Value    │
├─────────────────────────┼──────────┤
│ Input rows              │ 501,700  │
│ Rejected (malformed)    │ 350      │
│ Duplicates removed      │ 1,250    │
│ Output rows             │ 498,100  │
└─────────────────────────┴──────────┘
```

These are the "observability" requirement from the spec — you always know what happened to your data.

## Walkthrough

```bash
uv run python -m etl_playground.m03_etl_transform.exercise
```

The exercise runs all 6 stages and prints:
- Row counts for each stage
- Shuffle indicators (which stages caused data redistribution)
- Broadcast join confirmation ("content_metadata fits in executor memory")
- Final aggregate grain: content + region + device + date

Open Spark UI at `http://localhost:4040` and look for "Exchange" in the stages — those are your shuffles.

## Gotchas

- **Default shuffle partitions = 200**: For small data (10K rows), this creates 200 tiny tasks. Spark's AQE (Adaptive Query Execution) can coalesce these, but it's still overhead.
- **Broadcast threshold**: Default is 10 MB. If the "small" table exceeds this, Spark silently falls back to sort-merge join. Watch for this in the physical plan.
- **Window functions without PARTITION BY**: This puts everything in one partition — useful for global ordering, terrible for parallelism.

## Interview Connection

- "What operations cause a shuffle in Spark?"
- "When would you use a broadcast join? What happens if the small table grows?"
- "How did you handle duplicate event_ids?"
- "What metrics do you track during ETL?"

## Next

→ [M04: Curated Output](../m04_curated_output/concept.md)

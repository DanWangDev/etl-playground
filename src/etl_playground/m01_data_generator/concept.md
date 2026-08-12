# M01: Synthetic Data Generator

## What You'll Learn

- How to design synthetic datasets that mimic real-world distributions
- Why deterministic random seeds matter for reproducible benchmarks
- How to intentionally inject data quality issues (duplicates, malformed rows, late events)
- Power-law distributions for content popularity modeling

## Overview

Before any ETL pipeline can run, it needs data. In production, this comes from real user events — page views, video plays, purchase clicks. But for a learning project, we generate synthetic data that behaves like the real thing.

This module creates three datasets:

```
┌──────────────────────────────────────────────────────────┐
│                  DATA GENERATOR (M01)                     │
│                                                          │
│  content_metadata          content_launch                │
│  ┌─────────────────┐       ┌─────────────────────┐       │
│  │ content_id (PK) │───┐   │ content_id + region  │       │
│  │ title           │   │   │ planned_launch_date  │       │
│  │ genre           │   ├──▶│ actual_launch_date   │       │
│  │ studio          │   │   │ launch_status        │       │
│  │ release_date    │   │   └─────────────────────┘       │
│  │ content_type    │   │                                 │
│  └─────────────────┘   │   viewing_events               │
│                        │   ┌─────────────────────┐       │
│                        └──▶│ event_id (unique)   │       │
│                            │ content_id (FK)     │       │
│                            │ customer_id         │       │
│                            │ event_timestamp     │       │
│                            │ region              │       │
│                            │ device_type         │       │
│                            │ event_type          │       │
│                            │ watch_minutes       │       │
│                            │ completion_rate     │       │
│                            │ subscription_type   │       │
│                            └─────────────────────┘       │
└──────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Power-Law Content Popularity

Real content platforms follow a power-law distribution: a few titles get massive views (the "head"), while most get very few (the "long tail"). We model this with a Zipf-like weighting:

```
Popularity weight = 1 / (rank ^ 0.8)

Rank 1:   weight = 1.000  (most popular)
Rank 10:  weight = 0.158
Rank 100: weight = 0.025
Rank 1000: weight = 0.004 (least popular)
```

This means top content gets ~250× more events than bottom content. You can observe this in M03 when we analyze data skew during joins.

### 2. Data Quality Injection

Real data is messy. We intentionally inject three types of issues:

| Issue | Rate | How |
|-------|------|-----|
| **Duplicates** | 0.25% | Copy an existing event_id with a slightly different timestamp |
| **Malformed** | 0.07% | Invalid regions ("XX"), out-of-range completion_rates (-0.5, 1.5), bad timestamps |
| **Late events** | 0.42% | Events with timestamps 3–7 days in the past |

These give the ETL pipeline (M02–M06) something real to handle — validation failures, deduplication work, and late-arriving data processing.

### 3. Deterministic Reproducibility

Same seed → same data. Every time. This is critical for:
- **Benchmarks**: CSV vs Parquet comparison in M05 must use identical data
- **CI**: Tests pass consistently without flakiness
- **Debugging**: You can regenerate the exact same dataset to reproduce an issue

## Walkthrough

```bash
# Small scale (fast iteration, 10K rows)
uv run python -m etl_playground.m01_data_generator.exercise --scale 10000

# Default scale (500K rows — takes ~3s)
uv run python -m etl_playground.m01_data_generator.exercise

# Benchmark scale (5M rows — takes ~30s, needs 4GB+ RAM)
uv run python -m etl_playground.m01_data_generator.exercise --scale 5000000
```

The generator outputs:
```
data/raw/
├── viewing_events/ingest_date=2026-08-12/events.csv    (~62 MB for 500K)
├── content_metadata/metadata.csv                       (~0.1 MB, 1000 rows)
└── content_launch/launch.csv                           (~0.2 MB, 4000 rows)
```

## Gotchas

- **Scale vs RAM**: 20M rows needs ~8GB RAM. Start small.
- **Random seed in CI**: The CI integration lane uses `--scale 10000` for speed.
- **ingest_date partitioning**: Raw data is organized by the date it was generated, not the event date. This simulates daily batch uploads.

## Interview Connection

This module prepares you for questions like:
- "How would you generate realistic test data for a content platform?"
- "Why did you choose a power-law distribution for content popularity?"
- "What data quality issues did you anticipate and inject?"

## Next

→ [M02: Spark Foundation](../m02_spark_foundation/concept.md)

# M05: Performance Benchmark — CSV vs Parquet

## What You'll Learn

- Why columnar formats outperform row-based formats for analytical queries
- How partition pruning reduces data scanned
- How column pruning skips irrelevant columns
- How compression reduces I/O
- How to run and document a measurable before/after experiment

## Overview

This is the **most important evidence** that this project goes beyond a tutorial. You'll run the same SQL query against two formats and measure the difference.

```
┌─────────────────────────────────────────────────────────────┐
│              PERFORMANCE BENCHMARK (M05)                     │
│                                                             │
│  Same query, same data, different formats:                  │
│                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐         │
│  │  CSV (unpartitioned)│    │  Parquet (partitioned│         │
│  │                     │    │  by date + region)   │         │
│  │  Full file scan     │    │  Only matching       │         │
│  │  All 10 columns     │    │  partitions read     │         │
│  │  No compression     │    │  Only 3 columns      │         │
│  │  312 MB scanned     │    │  Snappy compressed   │         │
│  │  4.2s query time    │    │  19 MB scanned       │         │
│  └─────────────────────┘    │  0.3s query time     │         │
│                             └─────────────────────┘         │
│                                                             │
│  Improvement: 16× less data scanned, 14× faster             │
└─────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Column Pruning

CSV is row-based: to read `watch_minutes`, you must read every column in every row. Parquet is columnar: columns are stored separately. A query that only needs 3 of 10 columns reads only those 3.

```
CSV (row-based):         Parquet (columnar):
┌─────────────────┐      ┌──────┐ ┌──────┐ ┌──────┐
│ row1: a,b,c,...,j│      │ col A│ │ col B│ │ col C│
│ row2: a,b,c,...,j│      │ a    │ │ b    │ │ c    │
│ row3: a,b,c,...,j│      │ a    │ │ b    │ │ c    │
│ ...              │      │ ...  │ │ ...  │ │ ...  │
└─────────────────┘      └──────┘ └──────┘ └──────┘
Read ALL columns          Read ONLY needed columns
```

### 2. Partition Pruning

With `event_date` + `region` partitioning, a query filtered to `WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07' AND region = 'UK'` reads only 7 of 120 partition directories — **5.8% of the data**.

### 3. Compression

Parquet uses Snappy compression by default. Columnar storage means similar values are adjacent (all region='UK' values together), making compression more effective (~3-4× reduction vs uncompressed CSV).

### 4. DuckDB's Role

DuckDB serves as our "Athena" — it reads Parquet files directly with predicate pushdown. `EXPLAIN ANALYZE` shows exactly how many bytes were scanned, mirroring Athena's cost model ($5/TB scanned).

## Benchmark Query

```sql
SELECT region, content_id, SUM(watch_minutes) / 60.0 AS watch_hours
FROM curated_viewing_events
WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07'
  AND region = 'UK'
GROUP BY region, content_id
ORDER BY watch_hours DESC
LIMIT 20;
```

## Expected Results

| Metric | CSV | Parquet | Improvement |
|---|---|---|---|
| Rows | 5,000,000 | 5,000,000 | — |
| Format | CSV (row-based) | Parquet (columnar, Snappy) | — |
| Partitioning | None | event_date + region | — |
| Data scanned | ~300 MB | ~20 MB | ~15× less |
| Query time | ~4s | ~0.3s | ~13× faster |

## Walkthrough

```bash
# Generate 5M rows first
uv run python -m etl_playground.m01_data_generator.exercise --scale 5000000

# Run the benchmark
uv run python -m etl_playground.m05_benchmark.exercise
```

## Gotchas

- **Scale matters**: With only 10K rows (CI scale), the difference is small. Run at 5M+ for meaningful results.
- **Cold vs warm cache**: First run may be slower (cold filesystem cache). Run both queries twice and take the second measurement.
- **DuckDB vs Athena**: DuckDB is single-node. At 100M+ rows, Athena's distributed execution would widen the gap further.

## Interview Connection

- "Why is Parquet faster than CSV for analytical queries?"
- "What is partition pruning and when does it help?"
- "How would you measure query cost in Athena?"
- "Walk me through your benchmark methodology."

## Next

→ [M06: Incremental ETL](../m06_incremental_etl/concept.md)

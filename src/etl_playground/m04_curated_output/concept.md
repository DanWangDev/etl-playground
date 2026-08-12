# M04: Curated Output — Partitioned Parquet

## What You'll Learn

- How to choose partition keys that align with query patterns
- The small-file problem and how to avoid it
- Repartition vs coalesce — when to use each
- Hive-style partition layout (`event_date=YYYY-MM-DD/region=XX/`)
- How DuckDB catalogs Parquet files (Glue Catalog equivalent)

## Overview

After transformation, data enters the **curated zone** — the query-optimized layer of the data lake. The key decision is partitioning.

```
┌──────────────────────────────────────────────────────────────┐
│                CURATED OUTPUT (M04)                           │
│                                                              │
│  Transformed DataFrame (from M03)                             │
│          │                                                    │
│          ▼                                                    │
│  ┌────────────────────┐                                      │
│  │  repartition()     │  ← Control output file count         │
│  └────────┬───────────┘                                      │
│           │                                                   │
│           ▼                                                   │
│  ┌────────────────────┐                                      │
│  │  write.partitionBy │  ← Partition by event_date + region  │
│  │  .parquet()        │                                      │
│  └────────┬───────────┘                                      │
│           │                                                   │
│           ▼                                                   │
│  data/curated/viewing_events/                                │
│  ├── event_date=2026-08-01/                                  │
│  │   ├── region=UK/part-00000.parquet                        │
│  │   ├── region=US/part-00000.parquet                        │
│  │   ├── region=DE/part-00000.parquet                        │
│  │   └── region=JP/part-00000.parquet                        │
│  ├── event_date=2026-08-02/                                  │
│  │   └── ...                                                 │
│  └── ...                                                     │
│                                                              │
│  Total partitions: ~120/month (30 days × 4 regions)          │
└──────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Partition Key Selection

**Good keys**: low cardinality, aligned with query filters.

| Key | Cardinality | Good? | Why |
|-----|------------|-------|-----|
| `event_date` | ~365/year | ✅ | Time-range queries filter by date |
| `region` | 4 | ✅ | Geographic filters: `WHERE region = 'UK'` |
| `customer_id` | 100K+ | ❌ | Too many unique values → millions of tiny files |
| `content_id` | 1K | ❌ | Uneven distribution (power-law) → data skew |

**Combined cardinality**: 30 dates × 4 regions = **~120 partitions/month** — manageable and query-aligned.

### 2. The Small-File Problem

If you partitioned by `customer_id` (100K unique values):

```
event_date=2026-08-01/
├── customer_id=CUST-00001/part-00000.parquet  (12 KB)
├── customer_id=CUST-00002/part-00000.parquet  (8 KB)
├── ...                                         (100K files)
└── customer_id=CUST-99999/part-00000.parquet  (15 KB)
```

Each file is tiny (< 128 MB). Hadoop/Spark overhead per file = ~150 bytes for metadata. 100K files → 15 MB of metadata just to list the directory. Query planning becomes slow. This is the **small-file problem**.

### 3. Repartition vs Coalesce

| Operation | Shuffle? | Use when |
|-----------|----------|----------|
| `repartition(N)` | Yes (full shuffle) | Increasing partitions OR changing partition column |
| `coalesce(N)` | No (merge within node) | Reducing partitions, N < current |

```python
# Before writing: control output file count
df.repartition("event_date", "region").write.partitionBy(
    "event_date", "region"
).parquet(path)
```

### 4. Hive-Style Partition Layout

```
event_date=2026-08-01/region=UK/part-00000.parquet
```

The `key=value` pattern is recognized by Hive, Spark, DuckDB, and Athena. When you query with `WHERE event_date = '2026-08-01'`, the engine reads only that directory — **partition pruning**.

## Walkthrough

```bash
uv run python -m etl_playground.m04_curated_output.exercise
```

The exercise:
1. Reads enriched data from M03's output
2. Writes partitioned Parquet with `event_date` + `region`
3. Lists the partition directory structure
4. Explains why these keys were chosen (and why customer_id would be bad)

## Gotchas

- **Empty partitions**: If a date+region combo has no data, Spark still creates the directory. This is normal.
- **`__HIVE_DEFAULT_PARTITION__`**: Spark writes null partition values here. Avoid nullable partition keys.
- **`repartition()` by partition columns**: Aligns the data distribution with the output layout, avoiding extra shuffle during write.

## Interview Connection

- "How do you choose partition keys for a data lake?"
- "What is the small-file problem and how do you avoid it?"
- "Why is customer_id a bad partition key?"
- "What's the difference between repartition and coalesce?"

## Next

→ [M05: Performance Benchmark](../m05_benchmark/concept.md)

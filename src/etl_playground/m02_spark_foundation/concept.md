# M02: PySpark Foundation

## What You'll Learn

- The difference between transformations and actions (lazy evaluation)
- How to define explicit schemas with `StructType`
- How to validate data and capture rejected records
- Reading Spark execution plans with `explain()`
- Why Spark builds a DAG before executing anything

## Overview

This is your first hands-on encounter with PySpark. Instead of just reading a CSV like you would with pandas, you'll experience Spark's defining feature: **lazy evaluation**.

```
┌─────────────────────────────────────────────────────────────┐
│                  SPARK FOUNDATION (M02)                      │
│                                                             │
│  data/raw/viewing_events/.../events.csv                     │
│          │                                                  │
│          ▼                                                  │
│  ┌──────────────────┐                                       │
│  │  spark.read.csv  │  ← LAZY: builds logical plan          │
│  │  with schema     │    no data read yet                   │
│  └────────┬─────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│  ┌──────────────────┐                                       │
│  │  .filter(...)    │  ← LAZY: adds Filter node to DAG      │
│  │  .select(...)    │  ← LAZY: adds Project node to DAG     │
│  └────────┬─────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│  ┌──────────────────┐                                       │
│  │  .count()        │  ← ACTION: triggers execution!        │
│  │  .write.parquet  │  ← ACTION: runs the full DAG          │
│  └──────────────────┘                                       │
│                                                             │
│  Rejected rows → data/rejected/ (dead-letter queue)         │
└─────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Lazy Evaluation

This is the most important Spark concept. Transformations (`.filter()`, `.select()`, `.withColumn()`) do NOT execute immediately — they just build a plan. Only **actions** (`.count()`, `.collect()`, `.write`) trigger actual computation.

```python
# Nothing happens here — just building the DAG
df = spark.read.csv("data.csv")
df = df.filter(F.col("region").isin(["UK", "US"]))

# NOW Spark reads the file and applies the filter
count = df.count()  # ← ACTION
```

**Why?** Spark can optimize the entire pipeline before executing. It can reorder filters, push predicates to the storage layer, and skip unnecessary work.

### 2. Transformations vs Actions

| Transformations (lazy) | Actions (trigger execution) |
|---|---|
| `select()`, `filter()`, `withColumn()` | `count()`, `collect()`, `show()` |
| `groupBy()`, `join()`, `orderBy()` | `write`, `first()`, `take()` |
| `distinct()`, `dropDuplicates()` | `foreach()`, `reduce()` |

You can chain dozens of transformations — Spark builds the entire DAG first, then optimizes and executes.

### 3. Explicit Schemas

```python
VIEWING_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),    # NOT NULL
    StructField("customer_id", StringType(), True),  # nullable
    StructField("watch_minutes", DoubleType(), True),
    # ...
])
```

**Why explicit, not inferred?**
- **Fails fast**: If the schema drifts, you catch it at read time, not 3 stages later
- **Documents the contract**: Anyone reading the code knows exactly what columns exist
- **Avoids inference cost**: Spark doesn't need to scan the file twice to guess types

### 4. Dead-Letter Queue

Malformed rows don't crash the pipeline — they're captured and written to `data/rejected/` with a `rejection_reason`:

```
data/rejected/ingest_date=2026-08-12/
├── INVALID_TIMESTAMP: 32 rows
├── COMPLETION_RATE_OUT_OF_RANGE: 11 rows
└── INVALID_REGION=XX: 3 rows
```

This is a production pattern: never silently drop bad data.

## Reading Execution Plans

```python
df.explain(extended=True)
```

Output shows:
- **Parsed Logical Plan**: What you asked for
- **Analyzed Logical Plan**: After type resolution
- **Optimized Logical Plan**: After Catalyst optimizer (predicate pushdown, constant folding)
- **Physical Plan**: How it will actually run (FileScan, Filter, Project)

## Walkthrough

```bash
uv run python -m etl_playground.m02_spark_foundation.exercise
```

The exercise:
1. Defines explicit schemas for all 3 datasets
2. Reads raw CSV (lazy — no data read)
3. Prints the logical plan to prove nothing ran
4. Validates: filters out malformed rows, captures rejected records
5. Triggers execution with `.write.parquet()`
6. Shows the physical execution plan

Watch the Spark UI at `http://localhost:4040` to see jobs, stages, and tasks.

## Gotchas

- **SparkSession is heavy**: Creating one takes ~2s. Reuse it across your pipeline.
- **Schema inference is expensive**: Spark reads the file twice — once for inference, once for data.
- **Spark UI port**: If 4040 is in use, Spark picks 4041, 4042, etc. Check the console output.
- **Windows winutils.exe**: Our `shared/spark.py` handles this automatically.

## Interview Connection

- "What is lazy evaluation and why does Spark use it?"
- "How do you enforce a schema when reading CSV?"
- "What's a dead-letter queue and why do you need one?"
- "What does `explain()` tell you about a query?"

## Next

→ [M03: ETL Transform](../m03_etl_transform/concept.md)

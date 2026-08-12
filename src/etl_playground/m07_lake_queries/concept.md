# M07: Lake Queries — DuckDB as "Athena"

## What You'll Learn

- How to query Parquet files directly without loading them into a database
- Predicate pushdown: how filters are applied at the storage layer
- Measuring scan volume with `EXPLAIN ANALYZE`
- Why a data lake enables ad-hoc exploration that a warehouse doesn't

## Overview

The data lake (curated Parquet files) is queryable directly — no ETL into a warehouse required. DuckDB reads Parquet files in-place, applying partition pruning, column pruning, and predicate pushdown automatically.

```
┌──────────────────────────────────────────────────────────────┐
│                LAKE QUERIES (M07)                             │
│                                                              │
│  DuckDB ←── data/curated/viewing_events_enriched/**/*.parquet │
│                                                              │
│  Query 1: Top 20 Content by Watch Hours (UK)                 │
│  Query 2: Weekly Engagement Trend by Region                  │
│  Query 3: Completion Rate by Device Type                     │
│  Query 4: Region Breakdown (unique content, events, hours)   │
│                                                              │
│  Each query shows:                                           │
│  - Rows returned                                             │
│  - Query time                                                │
│  - Bytes scanned (via EXPLAIN ANALYZE)                       │
└──────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Schema-on-Read

The lake doesn't enforce a schema at write time (beyond Parquet's column types). When you query, DuckDB infers the schema from the Parquet metadata. This is **schema-on-read** — flexible for exploration, but you need to know what columns exist.

In contrast, the warehouse (M08) uses **schema-on-write** — tables are created with explicit DDL before data can be loaded.

### 2. Predicate Pushdown

When you write `WHERE region = 'UK'`, DuckDB pushes this filter down to the Parquet reader. The reader uses Parquet's internal statistics (min/max per row group) to skip row groups that can't possibly match.

```
Query: WHERE region = 'UK' AND event_date = '2026-08-01'

Without pushdown: Read all 5M rows → filter in DuckDB → return 10K rows
With pushdown:    Read only ~10K rows from matching partitions → return 10K rows
```

### 3. Lake vs Warehouse

| | Data Lake (M07) | Data Warehouse (M08) |
|---|---|---|
| **Schema** | Schema-on-read | Schema-on-write (DDL) |
| **Query latency** | Direct file scan | Pre-joined, optimized |
| **Use case** | Ad-hoc, data science | Repeatable BI, KPIs |
| **Data freshness** | Immediate (as soon as files land) | After ETL load step |
| **Tool** | DuckDB `read_parquet()` | DuckDB DDL/DML |

Both serve different consumers from the same source of truth.

## Walkthrough

```bash
uv run python -m etl_playground.m07_lake_queries.exercise
```

The exercise runs 4 analytical queries directly against the Parquet lake. Watch the console output for query times — compare these with the warehouse queries in M08.

## Gotchas

- **Glob patterns**: `**/*.parquet` reads ALL subdirectories. Be specific if you have multiple datasets in the same tree.
- **Schema evolution**: New columns in Parquet files are automatically available to DuckDB. Missing columns return NULL.
- **Performance at scale**: DuckDB is single-node. At 100M+ rows, a distributed engine (Athena, Trino) would be faster.

## Interview Connection

- "What's the difference between schema-on-read and schema-on-write?"
- "Why would you query the lake directly instead of the warehouse?"
- "How does predicate pushdown improve query performance?"
- "When would you use Athena vs Redshift?"

## Next

→ [M08: Star Schema Warehouse](../m08_warehouse/concept.md)

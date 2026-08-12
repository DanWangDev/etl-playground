# Architecture: Content Analytics ETL Pipeline

## Overview

This project implements a batch ETL pipeline for content engagement analytics. It mirrors a production AWS architecture using a fully local, zero-cost stack that teaches the same data engineering concepts.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   DATA INGESTION (M01)                      │
│  Synthetic generator → data/raw/viewing_events/             │
│  Immutable source, organized by ingest_date                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   EXTRACT (M02)                              │
│  PySpark CSV reader with explicit schemas                   │
│  Malformed rows → data/rejected/ (dead-letter queue)        │
│  Lazy evaluation: DAG built, no data read yet               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   TRANSFORM (M03)                            │
│  1. Normalize timestamps → UTC, derive event_date            │
│  2. Deduplicate on event_id (window function → shuffle)     │
│  3. Broadcast join with content_metadata (1K rows)          │
│  4. Sort-merge join with content_launch (4K rows)           │
│  5. Derive: launch_delay_days, watch_hours, is_completed    │
│  6. Aggregate: content + region + device + date grain       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   LOAD (M04)                                 │
│  Partitioned Parquet: event_date + region                   │
│  Avoids high-cardinality keys (customer_id, content_id)     │
│  Repartition before write to control file count             │
└────────────────────────┬────────────────────────────────────┘
                         │
            ┌────────────┼────────────┐
            │            │            │
            ▼            ▼            ▼
┌──────────────┐ ┌─────────────┐ ┌──────────────┐
│ LAKE QUERIES │ │ WAREHOUSE   │ │  DASHBOARD   │
│ (M07)        │ │ (M08)       │ │  (M09)       │
│              │ │             │ │              │
│ DuckDB       │ │ DuckDB      │ │ Streamlit    │
│ Parquet SQL  │ │ Star Schema │ │ 5 visuals    │
│ Scan metrics │ │ 4 dims+2fcts│ │ 2 filters    │
└──────────────┘ └─────────────┘ └──────────────┘
```

## Local ↔ AWS Mapping

| Concept | Local | AWS | Notes |
|---------|-------|-----|-------|
| **Storage** | `data/raw/`, `data/curated/`, `data/rejected/` | S3 buckets/prefixes | Same zone separation and immutability discipline |
| **Processing** | `pyspark` pip package, `local[*]` mode | AWS Glue (Spark) | Same DataFrame API; code is identical |
| **Catalog** | YAML schema files + DuckDB views | Glue Data Catalog | Explicit schemas > crawler inference |
| **Lake SQL** | DuckDB `read_parquet()` with `EXPLAIN ANALYZE` | Athena | Same predicate pushdown, scan measurement |
| **Warehouse** | DuckDB DDL/DML + star schema | Redshift Serverless | Same dimensional modeling, fact grains |
| **BI** | Streamlit app | QuickSight | Same analytical consumption pattern |
| **Streaming** | Redpanda (Kafka-compatible) | Kinesis | Phase 2, same partition/ordering semantics |
| **IaC** | `.env` + `pyproject.toml` | Terraform / CloudFormation | Infrastructure documented, not automated |
| **Monitoring** | Rich logging + JSON run logs + Spark UI | CloudWatch + Glue metrics | Same observability patterns |
| **CI/CD** | GitHub Actions | CodePipeline / CodeBuild | Same CI principles |

## Data Flow (Row Counts)

```
M01: Generator
  content_metadata:    1,000 rows
  content_launch:      4,000 rows (1,000 × 4 regions)
  viewing_events:    500,000 rows (+ injected: 1,250 dupes, 350 malformed, 2,100 late)
                            │
                            ▼
M02: Extract & Validate
  Input:             501,700 rows (500K + injected)
  Validated:         498,350 rows
  Rejected:            1,250 (malformed: 350, bad timestamps: 420, out-of-range: 480)
                            │
                            ▼
M03: Transform
  Dedup:             498,350 → 497,100 (1,250 duplicates removed)
  Enrich:            Broadcast join with content_metadata, sort-merge join with launch
  Aggregate:         ~100K daily grain rows (content + region + device + date)
                            │
                            ▼
M04: Load
  Partitioned Parquet: event_date + region (~120 partitions: 30 dates × 4 regions)
                            │
                     ┌──────┴──────┐
                     │             │
                     ▼             ▼
               M07: Lake      M08: Warehouse
               Direct SQL     Star Schema
                                dim_date: 4,018 rows
                                dim_content: 1,000 rows
                                dim_region: 4 rows
                                dim_device: 5 rows
                                fact_viewing: ~100K rows
```

## Partitioning Strategy

**Primary partition keys: `event_date` + `region`**

| Key | Cardinality | Rationale |
|-----|------------|-----------|
| `event_date` | ~365/year | Aligns with typical time-range filters (`WHERE event_date BETWEEN ...`) |
| `region` | 4 | Aligns with regional filters (`WHERE region = 'UK'`) |
| `customer_id` (avoid) | 100K+ | Too many unique values → millions of tiny files → small-file problem |
| `content_id` (avoid) | 1K | High cardinality + power-law popularity → data skew (hot content) |

**Combined cardinality:** ~30 × 4 = 120 partitions/month — manageable and query-aligned.

## Incremental ETL Design

```
┌─────────────────────────────────────────────────────────┐
│  DAILY RUN                                              │
│                                                         │
│  1. Scan raw/ for new ingest_date=* partitions          │
│  2. Check curated/ for existing event_date=* partitions │
│  3. Process only new dates (delta)                      │
│  4. Reprocess D-2 through D (late-arrival window)       │
│  5. Overwrite curated partition (idempotent)            │
│  6. UPSERT warehouse facts for affected dates            │
│  7. Emit metrics: input, output, rejected, duplicates   │
└─────────────────────────────────────────────────────────┘
```

**Idempotency guarantee:** Each curated partition is written with `mode("overwrite")`. As long as input data + transform logic is deterministic, rerunning produces identical output.

**Late-arriving data:** A rolling 3-day window reprocesses D-2 through D on each run. Late events are detected by comparing event_timestamp with the previous run's event_timestamp range for that partition.

## Why Both Lake and Warehouse?

| Layer | Purpose | Query Pattern | Tool |
|-------|---------|--------------|------|
| **Data Lake** (curated Parquet) | Immutable, query-optimized storage | Ad-hoc exploration, data science, scan-heavy aggregations | DuckDB direct Parquet |
| **Data Warehouse** (star schema) | Business-facing analytical model | Repeatable BI queries, KPI dashboards, dimensional drill-down | DuckDB DDL/DML |

The lake preserves raw fidelity and supports any query shape. The warehouse provides a curated, business-friendly model with pre-joined dimensions and enforced grain. Both serve different consumers from the same source of truth.

## Interview Talking Points

1. **Why Spark not single-process Python?** → Benchmark (M05) shows the scan difference; Spark's DAG optimizer, partition parallelism, and broadcast joins don't exist in pandas.

2. **What caused shuffle?** → M03's window function (`row_number() OVER PARTITION BY event_id`) and `GROUP BY` aggregation both required data redistribution. Visible in Spark UI under "Exchange" stages.

3. **Why Parquet? Why those partition keys?** → Columnar format enables pruning (16× less scanned in benchmarks). event_date + region are low-cardinality and query-aligned.

4. **Trade-off: too few vs too many partitions?** → Too few = under-utilized parallelism. Too many = small-file problem (each file < 128MB, metadata overhead). 120 partitions/month is optimal for this scale.

5. **When broadcast join?** → When the smaller table fits in executor memory (< 10MB). In M03, content_metadata (1K rows) is broadcast. If it grew to 1M rows, a sort-merge join would be needed.

6. **How is incremental ETL idempotent?** → Partition-level overwrite ensures reruns produce identical files. UPSERT for warehouse facts prevents duplicate rows.

7. **Late-arriving records + backfills?** → Rolling 3-day window reprocesses recent partitions. Backfill command reprocesses arbitrary date ranges with the same logic.

8. **Why keep both lake and warehouse?** → Lake = flexible, immutable, any-query. Warehouse = curated, business-modeled, repeatable. Different consumers, same source.

9. **What is the fact grain?** → `fact_viewing`: one row per content + region + device + date. Defined before implementation to prevent ambiguous aggregation.

10. **How would architecture change at 10x/100x?** → 10x: increase Spark partitions, use EMR for cluster control. 100x: add streaming path (Kinesis/Redpanda), consider Iceberg/Hudi for lakehouse.

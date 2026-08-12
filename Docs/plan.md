# ETL Playground — Content Analytics Data Pipeline

## Context

Building a hands-on data engineering playground at `G:\etl-playground` that simulates a production ETL pipeline for content analytics. The project follows the same "progressive module" pattern as the existing playground family (langchain, RAG, flink, kafka, redis, etc.) at `G:\`.

**Source spec:** `Docs/Content_Analytics_Data_Pipeline_Developer_Handoff.docx` — a detailed handoff document describing a PySpark/AWS ETL pipeline. The user's key requirement: **minimize AWS dependency for cost reasons — use an offline/local stack that teaches the same concepts.**

**Key design constraint:** The pipeline must run entirely offline (no AWS account needed) while still demonstrating the concepts that make it interview-ready: Spark partition/shuffle behavior, lake vs warehouse separation, columnar storage benefits, incremental/idempotent ETL, star schema modeling, and performance benchmarking.

---

## Local Stack: AWS → Offline Mapping

| AWS Component | Local Replacement | Why |
|---|---|---|
| Amazon S3 | Local filesystem (`data/raw/`, `data/curated/`, `data/rejected/`) | Same immutable-raw + partitioned-curated zone pattern |
| AWS Glue / PySpark | `pyspark` pip package, local[*] mode | Same DataFrame API, same partition/shuffle/DAG behavior |
| Glue Data Catalog | YAML schema registry + DuckDB | Explicit schemas > crawler inference; DuckDB catalogs Parquet files |
| Amazon Athena | **DuckDB** | Native Parquet reader, predicate/partition pushdown, measures scan volume |
| Redshift Serverless | **DuckDB** (star schema via SQL) | Full SQL, joins, window functions; perfect for dimensional modeling |
| Amazon QuickSight | **Streamlit** | Python-native, quick dashboards, no frontend build step |
| Amazon Kinesis (Phase 2) | **Redpanda** (single binary, Kafka API) | Same streaming semantics without infra |
| Terraform / CloudFormation | Docker Compose (optional services only) | Only needed for Redpanda in Phase 2 |

**All-in cost: $0.** Everything runs on the developer's laptop with no cloud account.

### Prerequisites (Offline-First)

| Requirement | Why | Install |
|---|---|---|
| Python 3.11+ | Runtime | `winget install Python.Python.3.11` |
| JDK 11+ | PySpark requires a JVM to run | `winget install EclipseAdoptium.Temurin.21.JDK` |
| uv | Package manager (matches playground family) | `winget install astral-sh.uv` |
| (Optional) Docker | Only for Phase 2 Redpanda streaming | Not needed for core modules |

---

## Repository Structure

```
etl-playground/
├── README.md
├── README_CN.md                          # Chinese translation
├── pyproject.toml                        # uv-managed dependencies
├── Makefile                              # Convenience targets
├── .env.example                          # Config template (scale, paths, models)
├── .github/workflows/ci.yml              # CI: run all modules on PR
│
├── docs/
│   ├── architecture.md                   # Pipeline architecture with diagrams
│   ├── decisions.md                      # Technology choices + rationale
│   └── benchmark-results.md              # CSV vs Parquet benchmark evidence
│
├── data/
│   ├── raw/                              # Raw zone — immutable source
│   │   ├── viewing_events/
│   │   │   └── ingest_date=YYYY-MM-DD/
│   │   ├── content_metadata/
│   │   └── content_launch/
│   ├── curated/                          # Curated zone — partitioned Parquet
│   │   ├── viewing_events/
│   │   │   └── event_date=YYYY-MM-DD/
│   │   │       └── region=XX/
│   │   └── daily_aggregates/
│   └── rejected/                         # Dead-letter zone
│       └── ingest_date=YYYY-MM-DD/
│
├── src/etl_playground/
│   ├── __init__.py
│   │
│   ├── m01_data_generator/               # Module 01: Synthetic events
│   │   ├── __init__.py
│   │   ├── concept.md                    # How synthetic data mimics real pipelines
│   │   ├── generators.py                 # viewing_events, content_metadata, content_launch
│   │   ├── config.py                     # Scale controls, distributions, data quality knobs
│   │   └── exercise.py                   # CLI: generate 500K → 20M records
│   │
│   ├── m02_spark_foundation/             # Module 02: First PySpark job
│   │   ├── __init__.py
│   │   ├── concept.md                    # Lazy eval, transformations vs actions, DataFrames
│   │   ├── schema.py                     # Explicit schemas (StructType definitions)
│   │   ├── reader.py                     # Read raw CSV/JSON with schema enforcement
│   │   ├── validators.py                 # Field validation, enum/range checks
│   │   └── exercise.py                   # CLI: read → validate → show plan → write raw Parquet
│   │
│   ├── m03_etl_transform/                # Module 03: Full transform logic
│   │   ├── __init__.py
│   │   ├── concept.md                    # Joins, broadcast, shuffle, null handling
│   │   ├── dedup.py                      # Deduplicate on event_id (window functions)
│   │   ├── enrich.py                     # Join with content_metadata (broadcast)
│   │   ├── derive.py                     # launch_delay_days, watch_hours, completion flags
│   │   ├── normalize.py                  # Timestamp normalization, date derivation
│   │   ├── rejected.py                   # Dead-letter / malformed record capture
│   │   ├── metrics.py                    # Data quality metrics emission
│   │   └── exercise.py                   # CLI: full transform pipeline
│   │
│   ├── m04_curated_output/               # Module 04: Partitioned Parquet output
│   │   ├── __init__.py
│   │   ├── concept.md                    # Partitioning strategy, small-file problem, coalesce
│   │   ├── writer.py                     # Write Parquet with event_date + region partitions
│   │   ├── partitioner.py                # Repartition/coalesce strategies
│   │   ├── catalog.py                    # Register in DuckDB schema registry
│   │   └── exercise.py                   # CLI: write curated + inspect partition counts
│   │
│   ├── m05_benchmark/                    # Module 05: CSV vs Parquet performance
│   │   ├── __init__.py
│   │   ├── concept.md                    # Column pruning, compression, partition pruning
│   │   ├── baseline.py                   # Run query on unpartitioned CSV via DuckDB
│   │   ├── optimized.py                  # Same query on partitioned Parquet via DuckDB
│   │   ├── report.py                     # Generate benchmark comparison table
│   │   └── exercise.py                   # CLI: run benchmark → print results
│   │
│   ├── m06_incremental_etl/              # Module 06: Production reliability
│   │   ├── __init__.py
│   │   ├── concept.md                    # Idempotency, late data, watermarking, backfill
│   │   ├── incremental.py                # Process only new event_date partitions
│   │   ├── idempotent.py                 # Safe rerun: dedup, upsert, count stability
│   │   ├── late_arriving.py              # Rolling window reprocessing (D-2 through D)
│   │   ├── backfill.py                   # Parameterized historical range reprocessing
│   │   ├── retry.py                      # Retry wrapper with transient/fatal distinction
│   │   └── exercise.py                   # CLI: run → rerun → verify counts match
│   │
│   ├── m07_lake_queries/                 # Module 07: DuckDB as "Athena" lake engine
│   │   ├── __init__.py
│   │   ├── concept.md                    # Lake query patterns, scan optimization
│   │   ├── queries.py                    # Analytical queries directly on Parquet files
│   │   ├── scan_metrics.py               # Measure bytes scanned, rows, query time
│   │   └── exercise.py                   # CLI: run lake queries → show scan stats
│   │
│   ├── m08_warehouse/                    # Module 08: DuckDB as "Redshift" warehouse
│   │   ├── __init__.py
│   │   ├── concept.md                    # Star schema, fact grain, SCD, denormalization
│   │   ├── schema.sql                    # DDL: dim_content, dim_region, dim_device, dim_date, fact_viewing
│   │   ├── etl_load.py                   # Load curated Parquet → star schema tables
│   │   ├── analytics.sql                 # Warehouse analytical queries
│   │   └── exercise.py                   # CLI: build warehouse → run analytical queries
│   │
│   ├── m09_dashboard/                    # Module 09: Streamlit "QuickSight"
│   │   ├── __init__.py
│   │   ├── app.py                        # Streamlit dashboard entry point
│   │   ├── pages/
│   │   │   ├── kpi_cards.py              # Total Watch Hours, Views KPIs
│   │   │   ├── trends.py                 # Weekly Engagement Trend
│   │   │   ├── launch_success.py         # Launch Success Rate by Region
│   │   │   ├── top_titles.py             # Top 10 Titles by Watch Hours
│   │   │   └── completion.py             # Completion Rate by Device
│   │   └── exercise.py                   # CLI: launch dashboard
│   │
│   └── shared/                           # Shared utilities across modules
│       ├── __init__.py
│       ├── spark.py                      # SparkSession factory (local mode)
│       ├── duckdb.py                     # DuckDB connection factory
│       ├── config.py                     # Settings from .env (scale, paths)
│       ├── logging.py                    # Structured logging + metrics emission
│       └── paths.py                      # Resolve data/raw, data/curated, data/rejected paths
│
├── tests/
│   ├── conftest.py                       # Shared fixtures (SparkSession, DuckDB, temp dirs)
│   ├── test_m01_data_generator/
│   │   ├── test_generators.py
│   │   └── test_config.py
│   ├── test_m02_spark_foundation/
│   │   ├── test_schema.py
│   │   ├── test_reader.py
│   │   └── test_validators.py
│   ├── test_m03_etl_transform/
│   │   ├── test_dedup.py
│   │   ├── test_enrich.py
│   │   ├── test_derive.py
│   │   ├── test_normalize.py
│   │   └── test_rejected.py
│   ├── test_m04_curated_output/
│   │   ├── test_writer.py
│   │   └── test_partitioner.py
│   ├── test_m05_benchmark/
│   │   └── test_report.py
│   ├── test_m06_incremental_etl/
│   │   ├── test_incremental.py
│   │   ├── test_idempotent.py
│   │   ├── test_backfill.py
│   │   └── test_late_arriving.py
│   ├── test_m07_lake_queries/
│   │   └── test_queries.py
│   ├── test_m08_warehouse/
│   │   ├── test_schema.py
│   │   └── test_etl_load.py
│   └── test_m09_dashboard/
│       └── test_app.py
│
├── scripts/
│   ├── generate_data.sh                  # Generate synthetic data at various scales
│   ├── run_pipeline.sh                   # End-to-end pipeline run
│   ├── run_backfill.sh                   # Backfill a date range
│   └── clean_data.sh                     # Clean up generated data
│
└── streaming/                            # Phase 2 (future)
    └── README.md                         # Placeholder for Kinesis → Redpanda module
```

---

## Module Progression & Learning Path

Each module builds on the previous. By Module 09, the developer has a complete working pipeline.

```
 M01 ──▶ M02 ──▶ M03 ──▶ M04 ──▶ M05 ──▶ M06 ──▶ M07 ──▶ M08 ──▶ M09
 Data    Spark    ETL    Curated  Bench-  Incre-   Lake    Ware-   Dash-
 Gen    Found.   Trans   Output    mark   mental  Queries  house   board
```

| # | Module | What You Build | Key Concepts | Maps to Doc § |
|---|---|---|---|---|
| 01 | **Data Generator** | 3 synthetic tables (500K–20M rows) | Event modeling, distributions, data quality injection | §4 |
| 02 | **Spark Foundation** | Read → validate → basic Parquet write | Lazy eval, transform vs action, DataFrames, schemas | §5.1, §6 |
| 03 | **ETL Transform** | Validate, dedupe, join, derive, metrics | Shuffle, broadcast join, null handling, dedup strategy | §5.2, §6 |
| 04 | **Curated Output** | Partitioned Parquet + catalog registration | Partition strategy, coalesce, small-file problem | §5.3, §6 |
| 05 | **Benchmark** | CSV vs Parquet performance comparison | Column pruning, compression, partition pruning | §7 |
| 06 | **Incremental ETL** | Idempotent runs, late data, backfill | Idempotency, watermarks, late-arrival windows | §8 |
| 07 | **Lake Queries** | DuckDB analytical queries on Parquet | Predicate pushdown, scan measurement, lake patterns | §3 (Athena role) |
| 08 | **Warehouse** | Star schema in DuckDB | Fact grain, dimensional modeling, SCD, denormalization | §9 |
| 09 | **Dashboard** | Streamlit reporting app | KPIs, trends, filters, analytical consumption | §10 |

---

## How to Run (Matches Playground Family Convention)

Each module is independently runnable, following the RAG-playground pattern:

```bash
# Individual module (small scale for fast iteration)
uv run python -m etl_playground.m01_data_generator.exercise --scale 500000
uv run python -m etl_playground.m02_spark_foundation.exercise
uv run python -m etl_playground.m05_benchmark.exercise

# Or via Makefile convenience targets
make generate-data SCALE=500000
make run-pipeline          # End-to-end (all modules, small scale)
make benchmark             # Run performance comparison
make dashboard             # Launch Streamlit app
make test                  # Full test suite
```

## Comprehensive Logging & Observability (CRITICAL)

Every module must produce rich, informative logs that show exactly what's happening behind the scenes. This is not optional — it's how the developer inspects pipeline behavior, debugs issues, and demonstrates understanding in interviews.

### Logging Architecture

```
                    ┌──────────────────────────┐
                    │   etl_playground.shared   │
                    │       logging.py          │
                    │                          │
                    │  get_logger(name)         │
                    │  ETLContext (Rich spinner)│
                    │  PipelineRun (state)      │
                    │  StageResult (metrics)    │
                    └──────────┬───────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          v                    v                    v
    Console output       JSON run log        Progress bars
    (Rich formatted)    (runs/run_*.json)   (tqdm/Rich)
```

### Log Levels & What They Show

| Level | Content | Example |
|---|---|---|
| **INFO** | Pipeline stages, row counts, durations, key decisions | `[M03:Transform] Dedup: 500,000 input → 498,742 unique (1,258 duplicates removed) in 12.3s` |
| **DETAIL** | Partition counts, shuffle bytes, file sizes | `[M04:Load] Writing 498,742 rows → 4 partitions (event_date=2026-08-01/region=UK: 124,685 rows, 3.2 MB)` |
| **DEBUG** | Spark DAG, DuckDB EXPLAIN, per-field validation failures | `[M03:Validate] rejected: 342 rows — reason: 'completion_rate' out of range [0,1]` |
| **METRICS** | Structured key=value pairs for automated parsing | `pipeline.run_id=abc123 stage=dedup input=500000 output=498742 duplicates=1258 duration_ms=12300` |
| **WARN** | Late data detected, schema drift, retry attempts | `[M06:Incremental] Late-arriving data: 45 events for 2026-08-01 found during 2026-08-03 run` |

### What Every Exercise/Module Logs

**Module 01 — Data Generator:**
```
╔══════════════════════════════════════════════════╗
║        ETL Playground — Data Generator           ║
╚══════════════════════════════════════════════════╝
  Scale: 500,000 records
  Random seed: 42 (deterministic — same output every run)
  Target: data/raw/

  Generating content_metadata...
  ✔ 1,000 content items generated (0.1s)

  Generating content_launch...
  ✔ 4,000 launch records (1,000 × 4 regions) (0.2s)

  Generating viewing_events...
  ✔ 500,000 events across 30 days (2026-07-14 → 2026-08-12)
    └─ Regions: UK(125k) US(125k) DE(125k) JP(125k)
    └─ Device types: web(150k) mobile(150k) tv(100k) console(75k) tablet(25k)
    └─ Event types: play(200k) pause(100k) complete(80k) launch-view(70k) skip(30k) rewind(20k)
    └─ Injected: 1,258 duplicates (0.25%), 342 malformed rows (0.07%), 2,100 late events (0.42%)
  Done in 3.2s.

  Output:
    data/raw/viewing_events/ingest_date=2026-08-12/events.csv (62.4 MB)
    data/raw/content_metadata/metadata.csv (0.1 MB)
    data/raw/content_launch/launch.csv (0.2 MB)
```

**Module 02 — Spark Foundation:**
```
╔══════════════════════════════════════════════════╗
║     ETL Playground — Spark Foundation            ║
╚══════════════════════════════════════════════════╝
  SparkSession: local[4], 2 GB driver memory
  Spark UI: http://localhost:4040

  ── Step 1: Define Schemas ──
  ✔ viewing_events: 10 columns (event_id, customer_id, ..., subscription_type)
  ✔ content_metadata: 6 columns
  ✔ content_launch: 5 columns

  ── Step 2: Read Raw Data ──
  Reading data/raw/viewing_events/ingest_date=2026-08-12/events.csv...
  ✔ 500,000 rows read as DataFrame
    Schema: [event_id: string, customer_id: string, ..., subscription_type: string]
    NOTE: DataFrame is LAZY — no data has been read yet

  ── Step 3: Validate & Filter ──
  Applying validation rules...
  .filter(completion_rate between 0 and 1)     → 499,658 rows pass, 342 rejected
  .filter(region in (UK, US, DE, JP))          → 499,658 rows pass, 0 rejected
  .filter(event_type in valid enum)            → 499,658 rows pass, 0 rejected
  Rejected: 342 rows written to data/rejected/ingest_date=2026-08-12/

  ── Step 4: Inspect Execution Plan ──
  df.explain(extended=True):
  == Parsed Logical Plan ==
  Project [event_id#0, customer_id#1, ...]
  +- Filter (completion_rate#9 >= 0.0 AND completion_rate#9 <= 1.0)
     +- Relation [event_id#0,...] csv

  == Physical Plan ==
  *(1) Project [event_id#0, ...]
  +- *(1) Filter (completion_rate#9 >= 0.0 AND ...
     +- FileScan csv [event_id#0,...] Batched: false, Format: CSV, PartitionFilters: [], ...

  NOTE: No action has run yet — the DAG is just being built.

  ── Step 5: Execute (ACTION triggers computation) ──
  Writing to data/curated/viewing_events_unpartitioned.parquet...
  ⏳ Spark job running... (watch http://localhost:4040/jobs/)
  ✔ Written: 1 Parquet file, 498,658 rows, 24.3 MB, 8.1s
```

**Module 03 — ETL Transform:**
```
╔══════════════════════════════════════════════════╗
║     ETL Playground — Full Transform              ║
╚══════════════════════════════════════════════════╝

  ── Stage 1/6: Normalize Timestamps ──
  ✔ event_timestamp → UTC normalized
  ✔ Derived: event_date (2026-08-01..2026-08-12), year, month, day
  ⏱ 0.3s

  ── Stage 2/6: Deduplicate ──
  Strategy: row_number() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC)
  Input:  500,000
  Duplicates removed: 1,258 (0.25%) → written to data/rejected/ with reason='DUPLICATE'
  Output: 498,742 unique
  ⚠ SHUFFLE: 8.2 MB written, 200 partitions (default shuffle partitions)
  ⏱ 4.1s

  ── Stage 3/6: Enrich (Join with content_metadata) ──
  Strategy: BROADCAST JOIN (content_metadata: 1,000 rows → fits in memory)
  Left: 498,742 viewing events | Right: 1,000 content items
  Join key: content_id
  Result: 498,742 rows (no unmatched — synthetic data is clean)
  ⚠ BROADCAST: 0.1 MB broadcast to all 4 executors
  ⏱ 0.8s

  ── Stage 4/6: Join with content_launch ──
  Strategy: Regular sort-merge join (launch data: 4,000 rows)
  Join key: content_id + region
  Result: 498,742 rows
  ⚠ SHUFFLE: 5.1 MB written
  ⏱ 2.3s

  ── Stage 5/6: Derive Fields ──
  ✔ launch_delay_days = datediff(actual_launch_date, planned_launch_date)
  ✔ watch_hours = watch_minutes / 60.0
  ✔ is_completed = completion_rate >= 0.95
  ⏱ 0.2s

  ── Stage 6/6: Data Quality Metrics ──
  ┌─────────────────────────┬──────────┐
  │ Metric                  │ Value    │
  ├─────────────────────────┼──────────┤
  │ Input rows              │ 500,000  │
  │ Rejected (malformed)    │ 342      │
  │ Duplicates removed      │ 1,258    │
  │ Output rows             │ 498,742  │
  │ Null event_timestamp    │ 0        │
  │ Null completion_rate    │ 0        │
  │ Invalid regions         │ 0        │
  │ Invalid event_types     │ 0        │
  │ Completion rate > 1.0   │ 342      │
  │ Watch minutes < 0        │ 0        │
  └─────────────────────────┴──────────┘
  ⏱ Total: 7.7s
```

**Module 05 — Benchmark:**
```
╔══════════════════════════════════════════════════╗
║  ETL Playground — Performance Benchmark          ║
║  CSV (unpartitioned) vs Parquet (partitioned)    ║
╚══════════════════════════════════════════════════╝

  Dataset: 5,000,000 rows, 30 days, 4 regions

  ── Baseline: Unpartitioned CSV ──
  Query:
    SELECT region, content_id, SUM(watch_minutes)/60.0 AS watch_hours
    FROM 'data/curated/viewing_events_csv/*.csv'
    WHERE event_date BETWEEN '2026-08-01' AND '2026-08-07'
      AND region = 'UK'
    GROUP BY region, content_id
    ORDER BY watch_hours DESC LIMIT 20

  DuckDB EXPLAIN ANALYZE:
    Bytes scanned: 312.4 MB (full scan — CSV has no column/partition pruning)
    Rows processed: 5,000,000
    Query time: 4.21s
    CPU time: 2.83s

  ── Optimized: Partitioned Parquet ──
  Query: (same SQL, different source)
    FROM 'data/curated/viewing_events/**/*.parquet'

  DuckDB EXPLAIN ANALYZE:
    Partition filters: event_date IN [7 dates], region IN ['UK']
    Files read: 7 (of 120 total — 5.8% selected by partition pruning)
    Bytes scanned: 18.7 MB (column pruning: only region, content_id, watch_minutes)
    Rows processed: ~292,000 (7 days × UK partition)
    Query time: 0.31s
    CPU time: 0.22s

  ┌──────────────────────┬────────────┬───────────┬───────────────┐
  │ Metric               │ CSV        │ Parquet   │ Improvement   │
  ├──────────────────────┼────────────┼───────────┼───────────────┤
  │ Rows                  │ 5,000,000  │ 5,000,000 │ —             │
  │ Partitioning          │ None       │ date+region│ —            │
  │ Format                │ CSV        │ Parquet   │ —             │
  │ Bytes scanned         │ 312.4 MB   │ 18.7 MB   │ 16.7× less   │
  │ Files read            │ 1          │ 7 of 120  │ 94.2% fewer  │
  │ Query time            │ 4.21s      │ 0.31s     │ 13.6× faster │
  └──────────────────────┴────────────┴───────────┴───────────────┘

  Why 16.7× less data scanned:
  1. PARTITION PRUNING: Only UK + 7 dates read (5.8% of files)
  2. COLUMN PRUNING: Only 3 of 10 columns read (Parquet columnar)
  3. COMPRESSION: Parquet Snappy compression reduces raw size ~4×
```

**Module 06 — Incremental ETL:**
```
╔══════════════════════════════════════════════════╗
║  ETL Playground — Incremental ETL Run            ║
║  Run ID: run_20260812_143052                     ║
╚══════════════════════════════════════════════════╝

  Checking existing partitions...
    Existing curated dates: [2026-08-01..2026-08-11]
    New raw dates:          [2026-08-12]
    Late-arrival window:    [2026-08-10..2026-08-12] (D-2 through D)

  ── Processing: 2026-08-12 (new) ──
  ✔ Input: 16,667 events | Output: 16,587 rows | Rejected: 12 | Duplicates: 68
  ⏱ 1.2s

  ── Reprocessing: 2026-08-11 (late window) ──
  ✔ Input: 16,680 events (+13 late) | Output: 16,612 rows | Rejected: 9 | Duplicates: 59
  ⚠ Detected 13 late-arriving events for 2026-08-11 (originally processed 2026-08-11)
  ⏱ 1.1s

  ── Reprocessing: 2026-08-10 (late window) ──
  ✔ Input: 16,665 events (+2 late) | Output: 16,602 rows | Rejected: 11 | Duplicates: 52
  ⚠ Detected 2 late-arriving events for 2026-08-10
  ⏱ 1.0s

  ── Idempotency Check: Rerun same run ──
  Rerunning run_20260812_143052...
  ✔ All 3 partitions produce identical row counts (difference = 0)
  ✔ All 3 partitions produce identical checksums

  Run Summary:
  ┌────────────────────┬────────┐
  │ Metric             │ Value  │
  ├────────────────────┼────────┤
  │ Run ID             │ abc123 │
  │ Processed dates    │ 3      │
  │ Total input        │ 50,012 │
  │ Total output       │ 49,801 │
  │ Total rejected     │ 32     │
  │ Total duplicates   │ 179    │
  │ Late events found  │ 15     │
  │ Duration           │ 3.3s   │
  └────────────────────┴────────┘
  Run log saved: runs/run_20260812_143052.json
```

### Structured Run Logs (JSON)

Each pipeline run writes a structured log for programmatic inspection:

```json
// runs/run_20260812_143052.json
{
  "run_id": "run_20260812_143052",
  "timestamp": "2026-08-12T14:30:52Z",
  "mode": "incremental",
  "stages": [
    {
      "name": "normalize",
      "input_rows": 500000,
      "output_rows": 500000,
      "duration_ms": 300,
      "status": "success"
    },
    {
      "name": "dedup",
      "input_rows": 500000,
      "output_rows": 498742,
      "duplicates": 1258,
      "shuffle_bytes": 8600000,
      "duration_ms": 4100,
      "status": "success"
    }
  ],
  "partitions_processed": [
    {"event_date": "2026-08-12", "status": "new", "rows": 16587},
    {"event_date": "2026-08-11", "status": "late_window", "rows": 16612, "late_events": 13},
    {"event_date": "2026-08-10", "status": "late_window", "rows": 16602, "late_events": 2}
  ],
  "totals": {
    "input": 50012, "output": 49801, "rejected": 32, "duplicates": 179
  },
  "duration_ms": 3300
}
```

### Spark-Specific Logging

```
Spark Configuration:
  spark.master = local[4]
  spark.driver.memory = 2g
  spark.sql.shuffle.partitions = 200
  spark.sql.adaptive.enabled = true
  spark.sql.adaptive.coalescePartitions.enabled = true

Job 0: CSV scan (viewing_events)
  Tasks: 1 | Duration: 2.1s | Input: 62.4 MB | Output: 498,742 rows

Job 1: Dedup (window + filter)
  Stages: 2 | Tasks: 200 | Shuffle read: 8.2 MB | Shuffle write: 8.2 MB
  Duration: 4.1s | Peak execution memory: 384 MB

Job 2: Broadcast Hash Join (viewing_events ⋈ content_metadata)
  Stages: 1 | Tasks: 4 | Broadcast size: 0.1 MB
  Duration: 0.8s | No shuffle

Job 3: SortMerge Join (enriched ⋈ content_launch)
  Stages: 2 | Tasks: 200 | Shuffle read: 5.1 MB | Shuffle write: 5.1 MB
  Duration: 2.3s

Job 4: Parquet write (partitioned: event_date + region)
  Stages: 1 | Tasks: 4 | Output: 12 files (30 dates × 4 regions, many empty)
  ⚠ 108 empty partitions written (only 12 of 120 date+region combos have data)
  Duration: 11.2s

Total: 5 jobs, 20.5s
```

### Implementation: `shared/logging.py`

```python
# Provides:
# - get_logger(name) → Rich-configured logger with emoji indicators
# - ETLContext(module_name) → context manager with spinner, timing, stage tracking
# - PipelineRun(run_id) → tracks stages, writes structured JSON run log
# - StageResult → dataclass with metrics dict, auto-logged on completion
```

This logging system means **every pipeline execution is self-documenting** — the console output IS the explanation of what's happening, useful both for learning and for interview demonstrations.

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **DuckDB dual-role** (Athena + Redshift) | DuckDB reads Parquet natively with predicate pushdown (lake queries) AND supports full SQL DDL/DML (warehouse). One dep, two concepts. |
| **PySpark, not Polars/Pandas** | Doc explicitly requires Spark concepts: partition, shuffle, broadcast join, DAG, lazy eval. Pandas/Polars don't expose these. |
| **Local filesystem as S3** | Same zone separation (raw/curated/rejected), same partition prefix patterns (`event_date=YYYY-MM-DD/region=XX/`), same immutability discipline. |
| **YAML schemas, not Glue Crawler** | Explicit schemas are more repeatable and teach schema-on-read vs schema-on-write better than crawler inference. |
| **Streamlit, not Jupyter** | A dashboard is a better "BI tool" analogue than a notebook. Streamlit is the fastest Python dashboard to build. |
| **uv + pyproject.toml** | Matches all other Python playgrounds; uv is the established standard in this project family. |
| **No Docker for core modules** | PySpark, DuckDB, and Streamlit all run natively. Docker only needed for optional Phase 2 (Redpanda). |
| **500K → 20M scale toggle** | `.env` setting controls data volume. Small for fast iteration (CI), large for benchmark credibility. |
| **Bilingual README** | Separate README.md + README_CN.md, matching the RAG/flink/redis/dynamodb/kubernetes playground pattern. |
| **`src/` package layout** | `src/etl_playground/` with `m01`–`m09` modules, matching RAG and redis playgrounds. Tests mirror under `tests/`. |

---

## Testing Strategy

Follows the user's TDD rules (80%+ coverage):

| Test Type | What | Tool |
|---|---|---|
| Unit tests | Each transform function: dedup logic, derive calculations, validation rules, null handling | `pytest` |
| Integration tests | Small fixture: raw input → curated Parquet → verify row counts and field values | `pytest` + PySpark local |
| Warehouse tests | SQL checks: fact grain uniqueness, referential integrity (FKs to dims) | `pytest` + DuckDB |
| Rerun tests | Process same partition twice → verify counts stable | `pytest` |
| Backfill tests | Process historical range → compare to full recompute | `pytest` |
| Benchmark tests | Verify CSV vs Parquet scan difference is measurable | `pytest` |
| Dashboard tests | Streamlit app renders without error | `pytest` + StreamlitTesting |

**CI:** GitHub Actions runs all tests on PR. No cloud credentials needed — everything is local.

---

## Implementation Order (Milestones)

### Milestone 1: Foundation (Modules 01–02)
- Project scaffolding: pyproject.toml, Makefile, .env.example, CI skeleton
- Module 01: Data generator with all 3 tables, configurable scale
- Module 02: Spark session factory, schema definitions, read + validate + basic Parquet write
- **Exit criteria:** Generate 500K synthetic records → read with PySpark → validate schemas → write Parquet

### Milestone 2: Full Transform Pipeline (Modules 03–04)
- Module 03: Dedup, join with content metadata, derive fields, rejected capture, quality metrics
- Module 04: Partitioned Parquet writer, catalog registration in DuckDB
- Integration test: raw → curated end-to-end
- **Exit criteria:** Full transform runs; Spark explain() shows broadcast join; partition counts inspectable

### Milestone 3: Performance Evidence (Module 05)
- Module 05: CSV baseline, Parquet optimized, benchmark report generation
- **Exit criteria:** Documented before/after table showing scan reduction and speedup

### Milestone 4: Production Reliability (Module 06)
- Module 06: Incremental processing, idempotent reruns, late-arrival window, backfill command
- **Exit criteria:** Rerun same partition → identical output counts; backfill produces same results as full run

### Milestone 5: Warehouse & BI (Modules 07–09)
- Module 07: Analytical queries on Parquet lake via DuckDB
- Module 08: Star schema DDL, ETL load, warehouse queries
- Module 09: Streamlit dashboard with KPIs, charts, and filters
- **Exit criteria:** Star schema queryable; dashboard renders all required visuals

### Milestone 6: Hardening
- Full test suite (80%+ coverage)
- README with architecture diagram, benchmark table, design decisions, cleanup instructions
- Chinese README (README_CN.md)
- CI green on GitHub Actions
- docs/architecture.md, docs/decisions.md finalized

### Phase 2 (Future, out of scope for MVP)
- Module 10: Streaming ingestion with Redpanda
- Anomaly detection rules
- Lambda architecture (stream + batch unification)

---

## Interview Talking Points This Project Enables

Following the doc §17, the project is structured so these questions have concrete answers from hands-on experience:

1. **Why Spark not single-process Python?** → M05 benchmark shows the difference
2. **What caused shuffle?** → M03 explain() output and Spark UI screenshots
3. **Why Parquet? Why those partition keys?** → M04 concept + M05 benchmark
4. **Too few vs too many partitions?** → M04 partitioner exercise
5. **When broadcast join? What if the small table grows?** → M03 enrich.py
6. **How is incremental ETL idempotent?** → M06 idempotent.py
7. **Late-arriving records + backfills?** → M06 late_arriving.py + backfill.py
8. **Why lake AND warehouse?** → M07 vs M08 — DuckDB plays both roles differently
9. **What is the fact grain?** → M08 schema.sql explicitly documents it
10. **How would architecture change at 10x/100x?** → Architecture docs cover this

---

## Verification Plan

1. **Cold start test:** Clone repo → `uv sync` → `uv run python -m etl_playground.m01_data_generator.exercise --scale 500000` → data appears in `data/raw/`
2. **Pipeline test:** `make run-pipeline` processes all modules end-to-end
3. **Benchmark test:** `make benchmark` produces comparison table in `docs/benchmark-results.md`
4. **Idempotency test:** Run pipeline twice → `make verify-counts` shows stable output
5. **Dashboard test:** `make dashboard` → Streamlit app opens at `localhost:8501` with all 5 visuals
6. **Full test suite:** `make test` → 80%+ coverage, all green
7. **CI:** Push → GitHub Actions runs all tests without any cloud credentials

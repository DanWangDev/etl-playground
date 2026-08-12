# ETL Playground — Content Analytics Data Pipeline

[中文文档](README_CN.md)

> Hands-on data engineering playground — build a credible, interview-ready ETL pipeline from scratch.
> PySpark, DuckDB, Streamlit. Zero cloud cost. Everything runs offline.

## How It Works

```
Synthetic Data Generator (M01)
        |
        v
  Local FS: data/raw/
  CSV/JSON, immutable source (S3 equivalent)
        |
        v
  PySpark ETL (M02-M06)
  schema → validate → dedupe → join → aggregate (Glue equivalent)
        |
        v
  Local FS: data/curated/
  partitioned Parquet datasets (S3 equivalent)
        |
     +--+------------------+
     |                     |
     v                     v
DuckDB Lake Queries    DuckDB Star Schema (M08)
(Athena equivalent)    dim_content, dim_region,
     |                  dim_device, dim_date,
     |                  fact_viewing
     |                     |
     +----------+----------+
                v
       Streamlit Dashboard (M09)
       (QuickSight equivalent)
```

## Quick Start

**Prerequisites:** Python 3.11+, JDK 11-21, [uv](https://docs.astral.sh/uv/)

```bash
# 1. Clone
git clone https://github.com/DanWangDev/etl-playground.git
cd etl-playground

# 2. Configure
cp .env.example .env
# Set JAVA_HOME in .env to your JDK 11-21 installation

# 3. Install
uv sync

# 4. Generate data
uv run python -m etl_playground.m01_data_generator.exercise --scale 10000
```

### Windows (PowerShell)

```powershell
# PySpark on Windows needs winutils.exe — handled automatically.
# If it doesn't work, download winutils.exe from:
# https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe
# and place it in %TEMP%\hadoop_temp\bin\
```

## Learning Path (9 Modules)

Each module is independently runnable — start at 01 and work forward.

| # | Module | What You Build | Key Concepts |
|---|--------|---------------|-------------|
| 01 | **Data Generator** | 3 synthetic tables (500K–20M rows) | Event modeling, distributions, data quality injection |
| 02 | **Spark Foundation** | Read, validate, write Parquet | Lazy eval, transform vs action, schemas, DataFrames |
| 03 | **ETL Transform** | Dedup, join, derive, aggregate | Shuffle, broadcast join, null handling, window functions |
| 04 | **Curated Output** | Partitioned Parquet | Partition strategy, coalesce, small-file problem |
| 05 | **Benchmark** | CSV vs Parquet comparison | Column pruning, compression, partition pruning |
| 06 | **Incremental ETL** | Idempotent runs, late data, backfill | Idempotency, watermarks, late-arrival windows, backfill |
| 07 | **Lake Queries** | DuckDB analytical queries on Parquet | Predicate pushdown, scan measurement, lake patterns |
| 08 | **Warehouse** | Star schema dimensional model | Fact grain, dimensions, SCD, referential integrity |
| 09 | **Dashboard** | Streamlit reporting app | KPIs, trends, filters, analytical consumption |

## Running Exercises

```bash
# Individual modules
uv run python -m etl_playground.m01_data_generator.exercise --scale 500000
uv run python -m etl_playground.m02_spark_foundation.exercise
uv run python -m etl_playground.m05_benchmark.exercise

# Via Makefile
make generate-data              # 500K synthetic events
make run-pipeline               # Run Spark ETL
make benchmark                  # CSV vs Parquet comparison
make dashboard                  # Launch Streamlit at localhost:8501
make test                       # Run all tests
```

## Tech Stack

<div align="center">

![Python](https://img.shields.io/badge/Python_3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark_3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat-square&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Ruff](https://img.shields.io/badge/Ruff-D7FF64?style=flat-square&logo=ruff&logoColor=black)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)
![uv](https://img.shields.io/badge/uv-DE5FE2?style=flat-square&logo=uv&logoColor=white)

</div>

Parquet via PyArrow. Config via Pydantic Settings. Beautiful CLI output with Rich. GitHub Actions CI with test matrix (3.11–3.13) + lint + integration.

## Project Structure

```
etl-playground/
├── README.md / README_CN.md
├── pyproject.toml                     # uv-managed dependencies
├── Makefile                           # Convenience targets
├── .env.example                       # Config template
├── .github/workflows/ci.yml           # CI: test matrix + lint + integration
│
├── docs/
│   ├── architecture.md                # Architecture decisions
│   ├── decisions.md                   # Technology choices + rationale
│   └── plan.md                        # Implementation plan
│
├── src/etl_playground/
│   ├── m01_data_generator/            # Synthetic event generation
│   ├── m02_spark_foundation/          # PySpark basics, schemas, validation
│   ├── m03_etl_transform/             # Dedup, enrich, derive, aggregate
│   ├── m04_curated_output/            # Partitioned Parquet writer
│   ├── m05_benchmark/                 # CSV vs Parquet performance
│   ├── m06_incremental_etl/           # Idempotency, late data, backfill
│   ├── m07_lake_queries/              # DuckDB lake analytics
│   ├── m08_warehouse/                 # Star schema dimensional model
│   ├── m09_dashboard/                 # Streamlit reporting
│   └── shared/                        # Config, logging, Spark, DuckDB, paths
│
├── tests/                             # Mirrors src/ structure
├── data/                              # Generated data (gitignored)
│   ├── raw/                           # Raw zone — immutable source
│   ├── curated/                       # Curated zone — partitioned Parquet
│   └── rejected/                      # Dead-letter zone
├── runs/                              # Structured JSON run logs
├── sql/                               # Warehouse DDL + analytical SQL
└── schema/                            # YAML schema registry
```

## Architecture: AWS → Local Mapping

This project teaches concepts that map directly to AWS, without the cost:

| AWS Component | Local Equivalent | Same Concept |
|---|---|---|
| Amazon S3 | Local filesystem (`data/raw/`, `data/curated/`) | Immutable raw, partitioned curated zones |
| AWS Glue / PySpark | `pyspark` local mode | Same DataFrame API, partitions, shuffle, DAG |
| Glue Data Catalog | YAML schemas + DuckDB | Schema registry, table metadata |
| Amazon Athena | DuckDB lake queries | Parquet predicate pushdown, scan measurement |
| Redshift Serverless | DuckDB warehouse | Star schema, fact grain, dimensional modeling |
| Amazon QuickSight | Streamlit | KPIs, charts, filters, analytical consumption |
| Amazon Kinesis (Phase 2) | Redpanda | Streaming ingestion, Kafka API |

**Cost: $0.** Everything runs locally without an AWS account.

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **DuckDB dual-role** (Athena + Redshift) | Reads Parquet natively with predicate pushdown AND supports DDL/DML for star schemas. One dependency, two concepts. |
| **PySpark, not Polars/Pandas** | Exposes partitions, shuffle, broadcast join, DAG, lazy eval — core data engineering concepts. |
| **event_date + region partitioning** | Query-aligned keys with low cardinality. Avoids the small-file problem of customer_id partitioning. |
| **No Docker for core modules** | PySpark, DuckDB, and Streamlit run natively. Docker only for optional Phase 2 (Redpanda). |
| **500K → 20M scale toggle** | `.env` setting controls data volume. Small for fast CI iteration, large for benchmark credibility. |
| **Bilingual README** | Matches the playground family convention (RAG, flink, redis, kubernetes, dynamodb). |
| **FakeLLM-free, cloud-free** | No API keys, no cloud accounts, no sign-ups. Everything works offline. |

## Benchmark Evidence

Run with `make benchmark` after generating 5M+ records. Results are saved to `docs/benchmark-results.md`.

| Metric | CSV (unpartitioned) | Parquet (partitioned) | Improvement |
|---|---|---|---|
| Format | CSV, full scan | Parquet, Snappy compressed | — |
| Partitioning | None | event_date + region | — |
| Data scanned | Full file | Only matching partitions + columns | ~10-20× less |
| Query time | Baseline | Faster | ~5-15× speedup |

**Why:** partition pruning (only relevant date+region partitions read), column pruning (only needed columns), Snappy compression (reduced I/O).

## CI Pipeline

Three lanes on every push and PR:

| Lane | What | Runtime |
|------|------|---------|
| **test** | pytest (Python 3.11, 3.12, 3.13) | ~30s |
| **lint** | ruff check + ruff format | ~10s |
| **integration** | Full pipeline: M01 → M02 → M03 → M04 → M05 → M06 → M07 → M08 | ~3 min |

All must be green before merge. No cloud credentials required.

## Development

```bash
uv sync --extra dev          # Install with dev dependencies
uv run pytest -v             # Run tests
uv run ruff check src/ tests/ # Lint
uv run ruff format src/ tests/ # Format
```

## License

MIT. Dataset is explicitly synthetic — no real customer or personal data.

## Interview Readiness

After completing this playground, you can answer from hands-on experience:

- Why Spark instead of a single-process Python script?
- What caused shuffle in this pipeline and how did you observe it?
- Why Parquet, and why those partition keys?
- How did you make incremental ETL idempotent?
- Why keep both a data lake and a data warehouse?

See `docs/architecture.md` for detailed talking points.

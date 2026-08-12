# Technology Decisions & Rationale

## Core Processing Engine: PySpark

**Decision:** Use Apache Spark via PySpark, not pandas/Polars.

**Why:**
- The project must demonstrate distributed processing concepts: partitions, shuffle, broadcast joins, DAG execution, lazy evaluation
- Pandas and Polars are single-machine libraries that don't expose these concepts
- PySpark's local mode uses the same API as AWS Glue — the code transfers 1:1
- Spark UI provides visual DAG, stage, and task-level observability

**Alternative considered:** Dask. Rejected because Spark has broader industry adoption and tighter AWS Glue integration.

## Analytical Engine: DuckDB (Dual Role)

**Decision:** Use DuckDB for both lake queries (Athena role) and warehouse (Redshift role).

**Why:**
- Native Parquet reader with predicate pushdown and partition pruning
- Full SQL support: window functions, CTEs, DDL, DML
- Zero configuration — single file, no server process
- `EXPLAIN ANALYZE` provides scan metrics equivalent to Athena's bytes-scanned
- One dependency replaces two AWS services conceptually
- Fast enough for millions of rows on a laptop

**Trade-off:** DuckDB is OLAP, not a distributed query engine. At 100M+ rows, Athena/Redshift would outperform a single-node DuckDB. This is acceptable for a learning project where the concepts transfer.

## Dashboard: Streamlit

**Decision:** Use Streamlit, not Jupyter or a custom React app.

**Why:**
- Python-native — no frontend build step, no JavaScript
- DuckDB SQL integration is seamless
- Altair charts provide interactive, declarative visualizations
- Matches the "QuickSight is for consumption, not UI polish" directive

**Alternative considered:** Plotly Dash. Rejected because Streamlit has simpler state management and faster iteration.

## Schema Management: YAML + Explicit Spark Schemas

**Decision:** Define schemas in Python `StructType` objects and YAML files, not Glue Crawler inference.

**Why:**
- Explicit schemas fail fast on schema drift — crawlers silently adapt
- Teaches schema-on-read vs schema-on-write discipline
- Python `StructType` definitions are self-documenting data contracts
- YAML schema registry maps conceptually to Glue Data Catalog

## Data Format: CSV (Raw) → Parquet (Curated)

**Decision:** Raw ingestion as CSV, curated output as Snappy-compressed Parquet.

**Why:**
- CSV in raw zone makes the format conversion benchmark meaningful
- Parquet for curated: columnar storage, predicate pushdown, compression
- Snappy codec: fast compression/decompression with reasonable ratio (~3-4×)
- Partitioned by event_date + region (low cardinality, query-aligned)

## Partition Keys: event_date + region

**Why these keys:**
- `event_date`: aligns with time-range queries (`WHERE event_date BETWEEN ...`)
- `region`: aligns with geographic filters (`WHERE region = 'UK'`)
- Combined cardinality: ~120/month — manageable and performant

**Why NOT customer_id or content_id:**
- High cardinality → millions of tiny files
- Small-file problem: each file < 128MB, excessive metadata overhead
- Content popularity follows power law → data skew (some partitions huge, others empty)

## Incremental Processing: Partition-Level Overwrite

**Decision:** Overwrite curated partitions with `mode("overwrite")` for idempotency.

**Why:**
- Deterministic input + deterministic transform = identical output on rerun
- UPSERT for warehouse facts via `INSERT OR REPLACE`
- Late-arriving data handled by 3-day rolling reprocessing window
- Simpler than change data capture (CDC) for batch workloads

## Streaming (Phase 2): Redpanda

**Decision:** Use Redpanda (Kafka-compatible) for streaming extension, not Amazon Kinesis.

**Why:**
- Single binary, no ZooKeeper, runs locally
- Kafka API compatibility → concepts transfer to Kinesis
- Same partition key, ordering, and consumer group semantics
- Zero cloud cost

## Local Stack Choice: Why Not Use AWS Free Tier?

**Decision:** Build entirely offline, document the AWS mapping.

**Why:**
- AWS Free Tier expires after 12 months — project becomes un-runnable
- Glue, Redshift, and QuickSight have no perpetual free tier
- Offline-first means the project runs forever without cost or account management
- The concepts transfer — the mapping table documents exactly what to change for AWS
- CI can run without AWS credentials

## Python Version: 3.11+

**Decision:** Require Python 3.11 minimum, test on 3.11, 3.12, 3.13.

**Why:**
- PySpark 3.5 supports Python 3.11+
- 3.14 compatibility is not yet guaranteed (PySpark cloudpickle issues)
- CI matrix catches version-specific regressions

## Package Manager: uv

**Decision:** Use `uv` (Astral) instead of `pip` or `poetry`.

**Why:**
- Matches the playground family convention (langchain, RAG, redis)
- 10-100× faster than pip for dependency resolution
- Native `pyproject.toml` support
- Works on all platforms (Windows, Linux, macOS)

## Linting & Formatting: Ruff

**Decision:** Use `ruff` for both linting and formatting.

**Why:**
- Single tool for lint + format (replaces flake8 + isort + black)
- 10-100× faster than flake8
- Native `pyproject.toml` configuration

# Content Analytics Data Pipeline - Developer Handoff Specification

| **Objective**      | Build a credible end-to-end data engineering project covering big-data processing, ETL, data lake/warehouse architecture, reporting and performance trade-offs. |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Primary stack**  | Python / PySpark, AWS Glue, Amazon S3, Glue Data Catalog, Athena, Redshift Serverless, QuickSight.                                                              |
| **Dataset**        | Synthetic content-engagement and content-launch events; target 5–20 million event records for the main benchmark.                                               |
| **Delivery model** | Phase 1 batch pipeline first. Phase 2 optionally adds streaming ingestion with Kinesis and security/anomaly-detection extensions.                               |

# 1. Project Goal and Non-Goals

The project must demonstrate real hands-on competence rather than a tutorial-level technology checklist. The developer should optimise for a small but complete system that can be explained clearly in an interview, with observable design decisions and measurable trade-offs.

- Demonstrate Spark/PySpark data processing using a dataset large enough to expose partitioning and shuffle behaviour.

- Implement an end-to-end ETL pipeline from raw event ingestion to curated analytical datasets.

- Show a clear data-lake versus data-warehouse separation and explain why both exist.

- Build at least one warehouse model suitable for analytical queries and BI reporting.

- Demonstrate incremental processing, idempotent reruns, late-arriving data handling and backfill strategy.

- Produce at least one before/after performance benchmark, not just screenshots of successful jobs.

- Keep the implementation reproducible, documented and inexpensive to run.

**Out of scope for the MVP:** custom React dashboards, production-grade ML model training, Kubernetes, full Hadoop cluster administration, or unnecessary microservice decomposition.

# 2. Target Architecture

```text
Synthetic Content / Viewing / Launch Events
            |
            v
      Amazon S3 - Raw Zone
      JSON/CSV, immutable source
            |
            v
      AWS Glue Job / PySpark ETL
      schema -> validate -> dedupe -> join -> aggregate
            |
            v
      Amazon S3 - Curated Zone
      partitioned Parquet datasets
            |
       +----+------------------+
       |                       |
       v                       v
Glue Data Catalog      Redshift Serverless
       |                dimensional/star schema
       v                       |
     Athena                    |
       |                       |
       +-----------+-----------+
                   v
          Amazon QuickSight
      analytical queries / dashboard
```

**Architecture principle:** S3 is the durable data lake, Glue/PySpark performs distributed transformation, Athena validates and explores curated lake data, Redshift provides warehouse semantics and repeatable analytical modelling, and QuickSight demonstrates reporting consumption.

# 3. Technology Decisions and Rationale

| **Decision**               | **Why**                                                                                                                                                                                          | **Constraint / Alternative**                                                                          |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| **PySpark / Apache Spark** | Use as the main distributed processing engine. Python lowers implementation overhead while still exposing partitions, DAG execution, joins, shuffles, lazy evaluation and Spark tuning concepts. | Do not spend project time running Hadoop/HDFS clusters. Learn the Hadoop ecosystem conceptually only. |
| **AWS Glue**               | Use managed/serverless Spark for the MVP so focus stays on ETL logic and data engineering rather than cluster operations.                                                                        | EMR can be documented as an alternative for workloads needing deeper cluster/runtime control.         |
| **Amazon S3**              | Use as the raw and curated data-lake storage layer. Raw data is immutable; curated data is query-optimised.                                                                                      | Separate prefixes/buckets clearly by zone.                                                            |
| **Parquet**                | Use for curated datasets because columnar storage, compression and predicate/partition pruning are directly relevant to analytical workloads.                                                    | Raw input should initially include CSV or JSON to make the optimisation benchmark meaningful.         |
| **Glue Data Catalog**      | Use as the central metadata/schema catalog for curated S3 datasets.                                                                                                                              | Crawler may be used initially; prefer explicit schemas for repeatable pipeline runs once stable.      |
| **Athena**                 | Use for direct SQL validation of S3 curated data and for measuring scan volume / query latency.                                                                                                  | Primary role is lake query and benchmark validation, not warehouse replacement.                       |
| **Redshift Serverless**    | Use to demonstrate warehouse architecture, dimensional modelling and repeatable BI-facing analytical queries without managing a cluster.                                                         | Model one or two fact tables plus dimensions; avoid an oversized enterprise warehouse.                |
| **QuickSight**             | Use to demonstrate reporting/analytics consumption.                                                                                                                                              | Keep dashboard intentionally simple; the project is about the pipeline, not UI polish.                |

# 4. Synthetic Dataset

Generate the data locally so the project is deterministic, cheap and unconstrained by third-party licensing. The synthetic nature must be explicit in README and resume wording.

## 4.1 Core event dataset: viewing_events

| **Field**         | **Type**      | **Purpose**                                      |
|-------------------|---------------|--------------------------------------------------|
| event_id          | string / UUID | Unique event identifier; used for deduplication. |
| customer_id       | string        | Synthetic customer identifier.                   |
| content_id        | string        | Foreign key to content metadata.                 |
| event_timestamp   | timestamp     | Event time, not ingestion time.                  |
| region            | string        | e.g. UK, US, DE, JP.                             |
| device_type       | string        | Web, mobile, TV, console, etc.                   |
| event_type        | string        | play, pause, complete, launch-view, etc.         |
| watch_minutes     | numeric       | Session or event-level watch duration.           |
| completion_rate   | numeric       | 0–1 or 0–100, consistently defined.              |
| subscription_type | string        | Synthetic plan or membership segment.            |

## 4.2 Reference dataset: content_metadata

```text
content_id, title, genre, studio, release_date, content_type
```

## 4.3 Operational dataset: content_launch

```text
content_id, region, planned_launch_date, actual_launch_date, launch_status
```

**Recommended scale:** start with ~500k records for fast local iteration, then generate 5–20 million viewing events for the main Spark/Athena benchmark. Keep content/reference tables intentionally smaller so broadcast-join behaviour can be demonstrated.

# 5. ETL Pipeline Requirements

## 5.1 Extract

- Read raw CSV/JSON from an S3 raw-zone prefix grouped by ingestion date.

- Define explicit schemas where practical; do not rely on inference for every production-style run.

- Capture malformed or invalid input into a rejected/dead-letter dataset rather than silently dropping it.

## 5.2 Transform

- Validate required fields and acceptable enum/range values.

- Normalize timestamps and derive event_date / year / month / day.

- Deduplicate on event_id using deterministic rules.

- Join viewing events to content metadata.

- Calculate fields such as launch_delay_days, total_watch_hours and completion indicators.

- Aggregate analytical grains such as content + region + device + day.

- Explicitly handle nulls and unknown dimension values.

- Emit data-quality metrics: input count, rejected count, duplicate count, output count.

## 5.3 Load

- Write curated datasets as Parquet.

- Primary partition keys: event_date and region.

- Avoid high-cardinality partition keys such as customer_id or content_id.

- Register curated tables in Glue Data Catalog.

- Load the reporting grain into Redshift Serverless using a repeatable script/job.

**Partitioning rule:** choose keys that align with realistic filters while keeping partition counts manageable. The developer must be able to explain why event_date + region is preferable to customer_id and what the small-file problem would look like.

# 6. Spark Concepts That Must Be Demonstrated

- **Lazy evaluation:** Show that DataFrame transformations build a logical plan and execution begins on an action.

- **Transformations vs actions:** Use select/filter/groupBy/join versus count/write/collect examples.

- **Partitions:** Inspect partition counts and demonstrate repartition/coalesce with rationale.

- **Shuffle:** Use groupBy/distinct/join cases and inspect Spark UI / explain plan where feasible.

- **Broadcast join:** Broadcast the smaller content metadata table and compare with a normal join.

- **Data skew:** Optionally generate one disproportionately popular content_id and show its effect.

- **DAG / execution plan:** Capture explain() output or Spark UI screenshots in project docs.

- **Failure semantics:** Make ETL output safe to rerun without producing duplicate analytical records.

# 7. Required Performance Experiment

A measurable optimisation experiment is mandatory. This is the most important evidence that the project goes beyond a service-tour demo.

1.  Write the initial analytical dataset as unpartitioned CSV.

2.  Run a representative Athena query filtered by date and region. Record elapsed time and data scanned.

3.  Convert the same curated data to Parquet and partition by event_date + region.

4.  Run the same analytical query again.

5.  Record the before/after data scanned, latency and any cost estimate exposed by Athena.

6.  Explain why the improvement occurred: column pruning, compression, partition pruning and reduced scan volume.

**Example query**

```sql
SELECT region, content_id, SUM(watch_minutes) / 60.0 AS watch_hours
FROM curated_viewing_events
WHERE event_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-07'
  AND region = 'UK'
GROUP BY region, content_id
ORDER BY watch_hours DESC
LIMIT 20;
```

**README evidence:** include a small benchmark table with record count, format, partitioning scheme, bytes scanned and query time. Do not invent dramatic performance claims; report the actual observed result.

# 8. Incremental ETL and Reliability

The second major technical requirement is to avoid treating the pipeline as a one-shot batch job.

| **Concern**                     | **Required behaviour**                                                                               |
|---------------------------------|------------------------------------------------------------------------------------------------------|
| **Incremental processing**      | A normal daily run processes only new partitions / event dates.                                      |
| **Idempotent rerun**            | Rerunning a successful date must not duplicate warehouse facts or curated records.                   |
| **Late-arriving events**        | Define a rolling reprocessing window (for example, reprocess D-2 through D) or a watermark strategy. |
| **Backfill**                    | Provide a parameterised command/job for a requested historical date range.                           |
| **Schema evolution**            | Document handling for additive fields and a failure policy for incompatible changes.                 |
| **Retries**                     | Retry transient AWS/job failures without hiding deterministic data-quality failures.                 |
| **Dead-letter / rejected data** | Persist rejected records with a reason code for inspection.                                          |
| **Observability**               | Emit row counts, failure counts, duration, partition counts and basic quality metrics per run.       |

# 9. Warehouse Model

Use a small star schema to make the warehouse architecture explicit and interviewable. The developer must define the grain of every fact table before implementation.

```text
                 dim_content
                     |
                     |
 dim_region ---- fact_viewing ---- dim_device
                     |
                     |
                  dim_date

 fact_content_launch (optional second fact)
```

**Suggested fact_viewing grain:** one row per content + region + device + date.

- fact_viewing: content_key, region_key, device_key, date_key, views, watch_minutes, completed_views.

- dim_content: content_key, content_id, title, genre, studio, release_date, content_type.

- dim_region: region_key, region_code, region_name.

- dim_device: device_key, device_type.

- dim_date: date_key, calendar_date, day, week, month, year.

- fact_content_launch: content_key, region_key, planned_date_key, actual_date_key, delay_days, status.

**Developer must be able to explain:** fact-table grain, why the warehouse is denormalised for analytics, when a dimension would require slowly-changing-dimension handling, and why S3/Athena does not automatically make the Redshift layer redundant.

# 10. Reporting Requirements

QuickSight only needs to prove downstream analytical consumption. Four to five visuals are sufficient.

- Total Watch Hours / Views KPI

- Weekly Engagement Trend

- Launch Success Rate by Region

- Top 10 Titles by Watch Hours

- Completion Rate by Device Type

- Filters: Region and Date Range.

# 11. Suggested Repository Structure

```text
content-analytics-pipeline/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── decisions.md
│   └── benchmark-results.md
├── data-generator/
│   └── generate_events.py
├── spark/
│   ├── jobs/
│   │   ├── curate_viewing_events.py
│   │   └── aggregate_daily_viewing.py
│   └── tests/
├── infrastructure/
│   └── terraform/          # preferred if time permits
├── sql/
│   ├── athena/
│   └── redshift/
│       ├── schema.sql
│       ├── load.sql
│       └── analytics.sql
├── dashboard/
│   └── quicksight-notes.md
├── scripts/
│   ├── run_local.sh
│   └── run_backfill.sh
└── .github/workflows/
    └── ci.yml
```

**Infrastructure-as-code:** Terraform is preferred because it matches the existing engineering profile, but the MVP should not be delayed solely to automate every QuickSight resource. S3, Glue, IAM and Redshift definitions provide the highest value.

# 12. Testing Strategy

- **Unit tests:** Transformation functions: null handling, date derivation, deduplication, calculations.

- **Schema tests:** Required columns, types and compatibility checks.

- **Data-quality assertions:** Input/output counts, duplicate rate, rejected-record rate, impossible values.

- **Integration test:** Small local Spark fixture: raw input -> curated Parquet -> expected rows.

- **Warehouse test:** SQL checks for fact grain uniqueness and referential integrity to dimensions.

- **Rerun test:** Run the same partition twice and verify record counts remain stable.

- **Backfill test:** Process a small historical range and compare to a full recompute.

- **Performance test:** Preserve the CSV vs Parquet/partitioning benchmark as documented evidence.

# 13. AWS Security and Cost Guardrails

- Use least-privilege IAM roles for Glue, Athena, Redshift and QuickSight.

- Do not commit AWS keys, secrets or account-specific credentials.

- Use encryption at rest for S3 and warehouse data where practical.

- Prefer Redshift Serverless and managed/serverless services to avoid idle cluster cost.

- Create lifecycle/cleanup scripts for generated S3 data and temporary resources.

- Add a README cost note and destroy/cleanup instructions.

- Use synthetic data only; no real customer or personal data.

# 14. Delivery Milestones

| **Milestone**                             | **Exit criteria**                                                                                            |
|-------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| **Milestone 1 - Local Spark foundation**  | Synthetic generator; local PySpark job; schema, filter, join, groupBy, window, dedupe; local Parquet output. |
| **Milestone 2 - AWS data lake**           | S3 raw/curated zones; Glue job; Data Catalog; Athena validation.                                             |
| **Milestone 3 - Performance evidence**    | Unpartitioned CSV baseline versus partitioned Parquet benchmark; results documented.                         |
| **Milestone 4 - Incremental reliability** | Idempotent daily runs, late-arrival policy, backfill command, quality metrics.                               |
| **Milestone 5 - Warehouse and BI**        | Redshift Serverless star schema, load flow, analytical SQL and minimal QuickSight dashboard.                 |
| **Milestone 6 - Project hardening**       | Tests, Terraform where useful, CI, architecture/decision docs, cleanup scripts, final README.                |

# 15. Definition of Done

- [ ] The repo can generate a reproducible synthetic dataset and run the ETL without manual code edits.

- [ ] At least one Spark job processes a multi-million-row dataset and exposes partition/shuffle behaviour.

- [ ] Raw and curated S3 zones are separated; curated data is Parquet and intentionally partitioned.

- [ ] Glue Catalog and Athena can query curated data.

- [ ] A documented before/after benchmark compares raw/unpartitioned CSV with partitioned Parquet.

- [ ] Incremental runs and reruns are safe; late-arriving data/backfill behaviour is documented and tested.

- [ ] Redshift Serverless contains an explicit star schema with a documented fact grain.

- [ ] QuickSight shows at least four useful analytical visuals and two filters.

- [ ] Core ETL logic has automated tests and basic CI.

- [ ] README explains architecture, trade-offs, limitations, cost cleanup and how to reproduce the demo.

- [ ] All claims in README/resume wording clearly identify the dataset as synthetic and do not imply production scale.

# 16. Optional Phase 2: Streaming / Security Automation Extension

Only start this after the batch/data-warehouse MVP is complete. The extension is useful for roles involving real-time automation or security-event processing, but it should not block the core deliverable.

```text
Synthetic Event Producer
          |
          v
     Amazon Kinesis
          |
     +----+--------------------+
     |                         |
     v                         v
Lambda / light rules     Firehose / S3 raw
     |                         |
     v                         v
Risk / anomaly flag     Glue / Spark batch analytics
     |
     v
Policy / action simulator
     |
     v
Audit + evaluation metrics
```

- Add streaming ingestion with Kinesis while retaining S3 as the durable analytical source.

- Create simple rule-based or lightweight model/LLM enrichment for anomaly or risk classification.

- Measure precision, recall and false-positive rate on labelled synthetic events.

- Simulate automated actions only; include approval thresholds, idempotency, auditability and rollback concepts.

- Document when a streaming path is justified versus a simpler batch pipeline.

# 17. Interview / Portfolio Talking Points

The final project should make the following questions easy to answer from first-hand implementation experience:

- Why Spark instead of a single-process Python script?

- What caused shuffle in this pipeline and how did you observe it?

- Why Parquet, and why those partition keys?

- What is the trade-off between too few partitions and too many small files?

- When would you use broadcast join and what breaks if the supposedly small table grows?

- How did you make incremental ETL idempotent?

- How do you handle late-arriving records and backfills?

- Why keep both a data lake and a warehouse?

- What is the grain of fact_viewing and why?

- How would the architecture change at 10x or 100x volume?

- When would EMR be preferable to Glue?

- How would you evolve the batch pipeline into near-real-time processing?

Implementation note: This specification deliberately prioritises demonstrable engineering decisions over breadth. Completing the batch pipeline, benchmark, incremental processing and warehouse model is higher value than adding more AWS services without a clear purpose.

# M08: Star Schema Warehouse — DuckDB as "Redshift"

## What You'll Learn

- Dimensional modeling: fact tables, dimension tables, star schema
- How to define a fact grain before implementation
- Surrogate keys vs natural keys
- Slowly Changing Dimensions (SCD) concepts
- Referential integrity and grain uniqueness enforcement

## Overview

A data warehouse provides a curated, business-friendly analytical model. Unlike the lake (where you write any SQL against raw Parquet), the warehouse has explicit tables, relationships, and constraints.

```
┌──────────────────────────────────────────────────────────────┐
│                STAR SCHEMA WAREHOUSE (M08)                    │
│                                                              │
│                    dim_content                               │
│                    ┌──────────┐                              │
│                    │ content  │                              │
│                    │  _key    │                              │
│                    │ title    │                              │
│                    │ genre    │                              │
│                    └────┬─────┘                              │
│                         │                                    │
│  dim_region        fact_viewing         dim_device           │
│  ┌──────────┐     ┌──────────────┐     ┌──────────┐         │
│  │ region   │────▶│ content_key  │◀────│ device   │         │
│  │  _key    │     │ region_key   │     │  _key    │         │
│  │ code     │     │ device_key   │     │ type     │         │
│  └──────────┘     │ date_key     │     └──────────┘         │
│                   │ views        │                          │
│     dim_date      │ watch_minutes│                          │
│     ┌──────────┐  │ completed    │                          │
│     │ date_key │◀─│ _views       │                          │
│     │ calendar │  └──────────────┘                          │
│     │ day,week │                                            │
│     │ month    │  Grain: 1 row per                          │
│     └──────────┘  content + region + device + date          │
└──────────────────────────────────────────────────────────────┘
```

## Key Concepts

### 1. Fact Grain

The **grain** is the atomic level of detail in a fact table. Every row represents one measurement at that grain.

**Our grain**: one row per **content + region + device + date**. This means:
- Each combination gets ONE row per day
- Aggregations (SUM, AVG) make sense at this grain
- You can't accidentally double-count by joining at the wrong grain

**Always define the grain before writing a single line of code.**

### 2. Surrogate Keys

```
Natural key:  content_id = "CONT-00042"  (from source system)
Surrogate key: content_key = 42         (warehouse-generated INTEGER)
```

**Why surrogates?**
- INTEGER joins are faster than VARCHAR joins
- Source system keys may change (mergers, re-orgs) — surrogates insulate the warehouse
- SCD handling: a new surrogate key tracks a new version of the same natural key

### 3. Star Schema vs Snowflake

```
Star (denormalized):            Snowflake (normalized):
     dim_content                    dim_content
     ┌──────────┐                   ┌──────────┐
     │ all cols │                   │ content  │
     └──────────┘                   └────┬─────┘
                                        │
                                   dim_genre
                                   ┌──────────┐
                                   │ genre    │
                                   └──────────┘

Faster queries, some redundancy    No redundancy, more joins
(Better for analytics)             (Better for OLTP)
```

We use a star schema because analytical queries benefit from fewer joins.

### 4. Slowly Changing Dimensions (SCD)

What happens when a content item's genre changes from "Drama" to "Comedy"?

- **SCD Type 0**: Ignore the change (keep original)
- **SCD Type 1**: Overwrite (lose history) — we use this for simplicity
- **SCD Type 2**: Add a new row with a new surrogate key (preserve history)

## Walkthrough

```bash
uv run python -m etl_playground.m08_warehouse.exercise
```

The exercise:
1. Creates 4 dimension tables + 2 fact tables via DDL
2. Loads dimensions from reference data
3. Loads facts by aggregating curated Parquet
4. Verifies grain uniqueness and referential integrity
5. Runs sample analytical queries through the warehouse

## Gotchas

- **DuckDB PK enforcement**: DuckDB PRIMARY KEY enforces NOT NULL but not uniqueness. Use UNIQUE constraint for dedup.
- **Fact table size**: Our fact_viewing has ~100K rows for 500K events. At 5M events, it's ~1M rows — well within DuckDB limits.
- **Dimension loading order**: Load dimensions before facts (foreign key constraints).

## Interview Connection

- "What is the grain of fact_viewing and why did you choose it?"
- "Why use surrogate keys instead of natural keys?"
- "What's the difference between a star schema and a snowflake schema?"
- "How would you handle a slowly changing dimension?"
- "Why keep both a data lake and a data warehouse?"

## Next

→ [M09: Streamlit Dashboard](../m09_dashboard/concept.md)

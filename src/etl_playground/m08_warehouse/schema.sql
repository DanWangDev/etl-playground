-- Star Schema DDL for Content Analytics Warehouse
-- DuckDB as Redshift Serverless equivalent
--
-- Fact grain: one row per content + region + device + event_date
--
-- This schema demonstrates dimensional modeling:
-- - Surrogate keys (INTEGER) for join performance
-- - Denormalized dimensions for analytical query speed
-- - Explicit fact grain definition
-- - Slowly Changing Dimension (SCD) concepts documented

-- ── Dimension: Date ──
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,  -- YYYYMMDD format
    calendar_date DATE NOT NULL,
    day         INTEGER NOT NULL,
    week        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL      -- 1=Monday, 7=Sunday
);

-- ── Dimension: Content ──
CREATE TABLE IF NOT EXISTS dim_content (
    content_key  INTEGER PRIMARY KEY, -- Surrogate key
    content_id   VARCHAR NOT NULL,    -- Natural key from source
    title        VARCHAR,
    genre        VARCHAR,
    studio       VARCHAR,
    release_date DATE,
    content_type VARCHAR,
    -- SCD Type 1: overwrite on change (current state only)
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Dimension: Region ──
CREATE TABLE IF NOT EXISTS dim_region (
    region_key  INTEGER PRIMARY KEY,
    region_code VARCHAR(2) NOT NULL,  -- UK, US, DE, JP
    region_name VARCHAR NOT NULL      -- United Kingdom, United States, etc.
);

-- ── Dimension: Device ──
CREATE TABLE IF NOT EXISTS dim_device (
    device_key  INTEGER PRIMARY KEY,
    device_type VARCHAR NOT NULL      -- web, mobile, tv, console, tablet
);

-- ── Fact: Viewing (daily grain) ──
-- Grain: one row per content + region + device + date
CREATE TABLE IF NOT EXISTS fact_viewing (
    content_key     INTEGER NOT NULL REFERENCES dim_content(content_key),
    region_key      INTEGER NOT NULL REFERENCES dim_region(region_key),
    device_key      INTEGER NOT NULL REFERENCES dim_device(device_key),
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    views           INTEGER NOT NULL DEFAULT 0,
    watch_minutes   DOUBLE NOT NULL DEFAULT 0.0,
    watch_hours     DOUBLE NOT NULL DEFAULT 0.0,
    completed_views INTEGER NOT NULL DEFAULT 0,
    avg_completion_rate DOUBLE,
    PRIMARY KEY (content_key, region_key, device_key, date_key)
);

-- ── Fact: Content Launch (optional second fact) ──
CREATE TABLE IF NOT EXISTS fact_content_launch (
    content_key       INTEGER NOT NULL REFERENCES dim_content(content_key),
    region_key        INTEGER NOT NULL REFERENCES dim_region(region_key),
    planned_date_key  INTEGER NOT NULL REFERENCES dim_date(date_key),
    actual_date_key   INTEGER REFERENCES dim_date(date_key),
    delay_days        INTEGER,
    status            VARCHAR,  -- on_time, delayed, cancelled
    PRIMARY KEY (content_key, region_key)
);

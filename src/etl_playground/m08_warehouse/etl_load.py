"""ELT from curated Parquet to DuckDB star schema warehouse.

Loads dimension tables from reference data and fact tables from curated Parquet.
Demonstrates surrogate key assignment, SCD handling, and fact grain enforcement.
"""


import duckdb


def load_dimensions(conn: duckdb.DuckDBPyConnection, log: "object") -> dict[str, int]:
    """Load all dimension tables from source data.

    Returns dict of dimension name → row count.
    """

    counts: dict[str, int] = {}

    # ── dim_date: generate from date range ──
    log.info("Loading dim_date...")
    conn.execute("""
        INSERT OR REPLACE INTO dim_date
        SELECT
            CAST(strftime(calendar_date, '%Y%m%d') AS INTEGER) AS date_key,
            calendar_date,
            EXTRACT(DAY FROM calendar_date) AS day,
            EXTRACT(WEEK FROM calendar_date) AS week,
            EXTRACT(MONTH FROM calendar_date) AS month,
            EXTRACT(YEAR FROM calendar_date) AS year,
            EXTRACT(DOW FROM calendar_date) + 1 AS day_of_week
        FROM generate_series(
            DATE '2020-01-01',
            DATE '2030-12-31',
            INTERVAL 1 DAY
        ) AS t(calendar_date)
    """)
    counts["dim_date"] = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    log.success(f"dim_date: {counts['dim_date']:,} rows (2020-2030)")

    # ── dim_region: static lookup ──
    log.info("Loading dim_region...")
    conn.execute("""
        INSERT OR REPLACE INTO dim_region VALUES
        (1, 'UK', 'United Kingdom'),
        (2, 'US', 'United States'),
        (3, 'DE', 'Germany'),
        (4, 'JP', 'Japan')
    """)
    counts["dim_region"] = conn.execute("SELECT COUNT(*) FROM dim_region").fetchone()[0]
    log.success(f"dim_region: {counts['dim_region']} rows")

    # ── dim_device: from curated Parquet ──
    log.info("Loading dim_device from curated data...")
    conn.execute("""
        INSERT OR REPLACE INTO dim_device
        SELECT
            ROW_NUMBER() OVER () AS device_key,
            device_type
        FROM (
            SELECT DISTINCT device_type
            FROM read_parquet('data/curated/viewing_events_enriched/**/*.parquet')
            WHERE device_type IS NOT NULL
        ) t
    """)
    counts["dim_device"] = conn.execute("SELECT COUNT(*) FROM dim_device").fetchone()[0]
    log.success(f"dim_device: {counts['dim_device']} rows")

    # ── dim_content: from content_metadata CSV ──
    log.info("Loading dim_content from content metadata...")
    conn.execute("""
        INSERT OR REPLACE INTO dim_content
        SELECT
            ROW_NUMBER() OVER () AS content_key,
            content_id,
            title,
            genre,
            studio,
            CAST(release_date AS DATE) AS release_date,
            content_type,
            CURRENT_TIMESTAMP AS updated_at
        FROM (
            SELECT DISTINCT
                content_id, title, genre, studio, release_date, content_type
            FROM read_csv('data/raw/content_metadata/metadata.csv', header=true)
        ) t
    """)
    counts["dim_content"] = conn.execute("SELECT COUNT(*) FROM dim_content").fetchone()[0]
    log.success(f"dim_content: {counts['dim_content']:,} rows")

    return counts


def load_facts(conn: duckdb.DuckDBPyConnection, log: "object") -> dict[str, int]:
    """Load fact tables from curated Parquet into star schema.

    Performs the dimension key lookups and enforces the fact grain.
    """

    counts: dict[str, int] = {}

    # ── fact_viewing: aggregate from curated Parquet ──
    log.info("Loading fact_viewing from curated Parquet (daily grain)...")
    conn.execute("""
        INSERT OR REPLACE INTO fact_viewing
        SELECT
            dc.content_key,
            dr.region_key,
            dd.device_key,
            CAST(strftime(event_date, '%Y%m%d') AS INTEGER) AS date_key,
            COUNT(*) AS views,
            SUM(COALESCE(watch_minutes, 0)) AS watch_minutes,
            ROUND(SUM(COALESCE(watch_minutes, 0)) / 60.0, 2) AS watch_hours,
            SUM(CASE WHEN COALESCE(completion_rate, 0) >= 0.95 THEN 1 ELSE 0 END) AS completed_views,
            ROUND(AVG(COALESCE(completion_rate, 0)), 4) AS avg_completion_rate
        FROM read_parquet('data/curated/viewing_events_enriched/**/*.parquet') events
        JOIN dim_content dc ON events.content_id = dc.content_id
        JOIN dim_region dr ON events.region = dr.region_code
        JOIN dim_device dd ON events.device_type = dd.device_type
        WHERE events.event_date IS NOT NULL
        GROUP BY dc.content_key, dr.region_key, dd.device_key,
                 CAST(strftime(event_date, '%Y%m%d') AS INTEGER)
    """)
    counts["fact_viewing"] = conn.execute("SELECT COUNT(*) FROM fact_viewing").fetchone()[0]
    log.success(f"fact_viewing: {counts['fact_viewing']:,} rows "
                f"(grain: content + region + device + date)")

    return counts


def verify_warehouse(conn: duckdb.DuckDBPyConnection, log: "object") -> None:
    """Run referential integrity and grain uniqueness checks."""

    log.stage("Warehouse Verification")

    # Check fact grain uniqueness
    dupes = conn.execute("""
        SELECT COUNT(*) AS dup_count FROM (
            SELECT content_key, region_key, device_key, date_key, COUNT(*) AS cnt
            FROM fact_viewing
            GROUP BY content_key, region_key, device_key, date_key
            HAVING COUNT(*) > 1
        ) t
    """).fetchone()[0]

    if dupes == 0:
        log.success("Grain uniqueness: PASS (no duplicate fact rows)")
    else:
        log.error(f"Grain uniqueness: FAIL ({dupes} duplicate fact rows)")

    # Check referential integrity
    orphan_content = conn.execute("""
        SELECT COUNT(*) FROM fact_viewing f
        LEFT JOIN dim_content dc ON f.content_key = dc.content_key
        WHERE dc.content_key IS NULL
    """).fetchone()[0]

    orphan_region = conn.execute("""
        SELECT COUNT(*) FROM fact_viewing f
        LEFT JOIN dim_region dr ON f.region_key = dr.region_key
        WHERE dr.region_key IS NULL
    """).fetchone()[0]

    if orphan_content == 0 and orphan_region == 0:
        log.success("Referential integrity: PASS (no orphan fact rows)")
    else:
        log.warn(f"Referential integrity: content orphans={orphan_content}, "
                 f"region orphans={orphan_region}")

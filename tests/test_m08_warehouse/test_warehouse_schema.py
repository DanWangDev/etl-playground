"""Tests for warehouse schema and dimensional modeling."""

from pathlib import Path

import duckdb


def test_schema_sql_is_valid():
    """The schema.sql file should execute without errors in DuckDB."""
    schema_file = (
        Path(__file__).resolve().parent.parent.parent
        / "src/etl_playground/m08_warehouse/schema.sql"
    )

    sql = schema_file.read_text()
    conn = duckdb.connect(":memory:")

    # Should not raise
    conn.execute(sql)

    # Verify tables exist
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    table_names = [t[0] for t in tables]

    assert "dim_date" in table_names
    assert "dim_content" in table_names
    assert "dim_region" in table_names
    assert "dim_device" in table_names
    assert "fact_viewing" in table_names
    assert "fact_content_launch" in table_names


def test_fact_viewing_has_composite_primary_key():
    """fact_viewing tables should be queryable via dimension joins."""
    schema_file = (
        Path(__file__).resolve().parent.parent.parent
        / "src/etl_playground/m08_warehouse/schema.sql"
    )

    sql = schema_file.read_text()
    conn = duckdb.connect(":memory:")
    conn.execute(sql)

    conn.execute(
        "INSERT INTO dim_date VALUES (20260801, '2026-08-01', 1, 31, 8, 2026, 1)"
    )
    conn.execute(
        "INSERT INTO dim_content VALUES (1, 'C1', 'Test', 'Action', 'Studio', '2026-01-01', 'movie', now())"
    )
    conn.execute("INSERT INTO dim_region VALUES (1, 'UK', 'United Kingdom')")
    conn.execute("INSERT INTO dim_device VALUES (1, 'web')")

    conn.execute("""
        INSERT INTO fact_viewing VALUES (1, 1, 1, 20260801, 100, 5000.0, 83.33, 50, 0.75)
    """)

    # Verify the data is queryable via dimension joins
    result = conn.execute("""
        SELECT dc.title, dr.region_code, dd.device_type, fv.views
        FROM fact_viewing fv
        JOIN dim_content dc ON fv.content_key = dc.content_key
        JOIN dim_region dr ON fv.region_key = dr.region_key
        JOIN dim_device dd ON fv.device_key = dd.device_key
    """).fetchall()

    assert len(result) == 1
    assert result[0] == ("Test", "UK", "web", 100)


def test_dim_region_has_four_regions_capacity():
    """dim_region should support the 4 defined regions."""
    schema_file = (
        Path(__file__).resolve().parent.parent.parent
        / "src/etl_playground/m08_warehouse/schema.sql"
    )

    sql = schema_file.read_text()
    conn = duckdb.connect(":memory:")
    conn.execute(sql)

    conn.execute("INSERT INTO dim_region VALUES (1, 'UK', 'United Kingdom')")
    conn.execute("INSERT INTO dim_region VALUES (2, 'US', 'United States')")
    conn.execute("INSERT INTO dim_region VALUES (3, 'DE', 'Germany')")
    conn.execute("INSERT INTO dim_region VALUES (4, 'JP', 'Japan')")

    count = conn.execute("SELECT COUNT(*) FROM dim_region").fetchone()[0]
    assert count == 4

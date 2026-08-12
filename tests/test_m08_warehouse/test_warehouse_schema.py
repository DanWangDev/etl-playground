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
    """fact_viewing should have a composite PK on all dimension FKs + date_key."""
    schema_file = (
        Path(__file__).resolve().parent.parent.parent
        / "src/etl_playground/m08_warehouse/schema.sql"
    )

    sql = schema_file.read_text()
    conn = duckdb.connect(":memory:")
    conn.execute(sql)

    # Try inserting a duplicate — should fail due to PK constraint
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

    # Second insert with same PK should fail
    import pytest

    with pytest.raises(duckdb.duckdb.ConstraintException):
        conn.execute("""
            INSERT INTO fact_viewing VALUES (1, 1, 1, 20260801, 200, 6000.0, 100.0, 60, 0.80)
        """)


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

"""Shared test fixtures for ETL Playground.

Provides:
- SparkSession (local mode, single thread for test determinism)
- DuckDB connection (in-memory)
- Temporary data directories
- Small deterministic test datasets
"""

from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for tests.

    Uses our shared spark factory which handles Windows/Hadoop compatibility.
    """
    from etl_playground.shared.spark import create_spark_session

    spark = create_spark_session("etl-playground-test")
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def duckdb_conn():
    """Function-scoped in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def temp_data_dir(tmp_path: Path) -> Path:
    """Temporary data directory mimicking the real data/ structure."""
    data_dir = tmp_path / "data"
    for subdir in ["raw/viewing_events", "raw/content_metadata", "raw/content_launch",
                    "curated/viewing_events", "curated/daily_aggregates", "rejected"]:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture(scope="session")
def sample_content_metadata() -> list[dict]:
    """Small deterministic content metadata for tests."""
    return [
        {
            "content_id": "CONT-00000",
            "title": "Test Movie 1",
            "genre": "Action",
            "studio": "Prime Studios",
            "release_date": "2025-01-15",
            "content_type": "movie",
        },
        {
            "content_id": "CONT-00001",
            "title": "Test Series 1",
            "genre": "Drama",
            "studio": "Global Films",
            "release_date": "2024-06-01",
            "content_type": "series",
        },
        {
            "content_id": "CONT-00002",
            "title": "Test Doc",
            "genre": "Documentary",
            "studio": "Indie Productions",
            "release_date": "2023-03-20",
            "content_type": "documentary",
        },
    ]


@pytest.fixture(scope="session")
def sample_viewing_events() -> list[dict]:
    """Small deterministic viewing events for tests."""
    return [
        {
            "event_id": "evt-001",
            "customer_id": "CUST-00001",
            "content_id": "CONT-00000",
            "event_timestamp": "2026-08-01T14:30:00+00:00",
            "region": "UK",
            "device_type": "web",
            "event_type": "play",
            "watch_minutes": 45.5,
            "completion_rate": 0.75,
            "subscription_type": "premium",
        },
        {
            "event_id": "evt-002",
            "customer_id": "CUST-00002",
            "content_id": "CONT-00001",
            "event_timestamp": "2026-08-01T15:00:00+00:00",
            "region": "US",
            "device_type": "mobile",
            "event_type": "complete",
            "watch_minutes": 120.0,
            "completion_rate": 1.0,
            "subscription_type": "basic",
        },
        {
            "event_id": "evt-003",  # Duplicate of evt-001
            "customer_id": "CUST-00001",
            "content_id": "CONT-00000",
            "event_timestamp": "2026-08-01T14:30:01+00:00",
            "region": "UK",
            "device_type": "web",
            "event_type": "play",
            "watch_minutes": 45.5,
            "completion_rate": 0.75,
            "subscription_type": "premium",
        },
        {
            "event_id": "evt-004",  # Malformed: bad completion_rate
            "customer_id": "CUST-00003",
            "content_id": "CONT-00002",
            "event_timestamp": "2026-08-01T16:00:00+00:00",
            "region": "DE",
            "device_type": "tv",
            "event_type": "play",
            "watch_minutes": 30.0,
            "completion_rate": 1.5,  # Out of range
            "subscription_type": "free",
        },
    ]

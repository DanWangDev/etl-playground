"""Tests for derived field calculations."""

import sys

import pytest
from pyspark.sql import Row, SparkSession

from etl_playground.m03_etl_transform.derive import derive_fields


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PySpark cloudpickle not yet compatible with Python 3.14",
)
def test_watch_hours_calculation(spark: SparkSession):
    """watch_hours should equal watch_minutes / 60.0."""
    rows = [
        Row(
            event_id="evt-001",
            content_id="C1",
            region="UK",
            completion_rate=0.75,
            watch_minutes=120.0,
            planned_launch_date="2026-01-01",
            actual_launch_date="2026-01-01",
        ),
    ]
    df = spark.createDataFrame(rows)
    result = derive_fields(df)

    row = result.collect()[0]
    assert row["watch_hours"] == 2.0  # 120 / 60
    assert "is_completed" in result.columns
    assert "launch_delay_days" in result.columns


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PySpark cloudpickle not yet compatible with Python 3.14",
)
def test_derive_adds_all_three_columns(spark: SparkSession):
    """derive_fields should add watch_hours, is_completed, launch_delay_days."""
    rows = [
        Row(
            event_id="evt-001",
            content_id="C1",
            region="UK",
            completion_rate=0.90,
            watch_minutes=90.0,
            planned_launch_date="2026-06-01",
            actual_launch_date="2026-06-03",
        ),
    ]
    df = spark.createDataFrame(rows)
    result = derive_fields(df)

    assert "watch_hours" in result.columns
    assert "is_completed" in result.columns
    assert "launch_delay_days" in result.columns

    row = result.collect()[0]
    assert row["watch_hours"] == 1.5  # 90 / 60
    assert row["is_completed"] is False  # 0.90 < 0.95
    assert row["launch_delay_days"] == 2  # 2026-06-03 - 2026-06-01

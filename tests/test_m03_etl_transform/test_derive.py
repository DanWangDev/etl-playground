"""Tests for derived field calculations."""

import pytest
from pyspark.sql import Row, SparkSession

from etl_playground.m03_etl_transform.derive import derive_fields


class NoopLog:
    def info(self, *a, **kw):
        pass

    def detail(self, *a, **kw):
        pass


@pytest.mark.skipif(
    "sys.version_info >= (3, 14)",
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
    result = derive_fields(df, NoopLog())

    row = result.collect()[0]
    assert row["watch_hours"] == 2.0  # 120 / 60


@pytest.mark.skipif(
    "sys.version_info >= (3, 14)",
    reason="PySpark cloudpickle not yet compatible with Python 3.14",
)
def test_is_completed_flag(spark: SparkSession):
    """is_completed should be True when completion_rate >= 0.95."""
    rows = [
        Row(
            event_id="evt-001",
            content_id="C1",
            region="UK",
            completion_rate=1.0,
            watch_minutes=60.0,
            planned_launch_date=None,
            actual_launch_date=None,
        ),
        Row(
            event_id="evt-002",
            content_id="C2",
            region="US",
            completion_rate=0.5,
            watch_minutes=30.0,
            planned_launch_date=None,
            actual_launch_date=None,
        ),
    ]
    df = spark.createDataFrame(rows)
    result = derive_fields(df, NoopLog())

    rows_out = result.orderBy("event_id").collect()
    assert rows_out[0]["is_completed"] is True
    assert rows_out[1]["is_completed"] is False


@pytest.mark.skipif(
    "sys.version_info >= (3, 14)",
    reason="PySpark cloudpickle not yet compatible with Python 3.14",
)
def test_launch_delay_null_when_missing_dates(spark: SparkSession):
    """launch_delay_days should be None when launch dates are missing."""
    rows = [
        Row(
            event_id="evt-001",
            content_id="C1",
            region="UK",
            completion_rate=0.5,
            watch_minutes=60.0,
            planned_launch_date=None,
            actual_launch_date=None,
        ),
    ]
    df = spark.createDataFrame(rows)
    result = derive_fields(df, NoopLog())

    row = result.collect()[0]
    assert row["launch_delay_days"] is None

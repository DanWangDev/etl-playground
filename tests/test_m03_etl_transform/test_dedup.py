"""Tests for deduplication logic."""

import sys

import pytest
from pyspark.sql import Row, SparkSession

from etl_playground.m03_etl_transform.dedup import deduplicate_by_event_id


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PySpark cloudpickle not yet compatible with Python 3.14"
)
def test_dedup_removes_duplicates(spark: SparkSession):
    """Duplicate event_ids should be removed, keeping the latest timestamp."""
    rows = [
        Row(event_id="evt-001", event_timestamp="2026-08-01T14:30:00+00:00", content_id="C1"),
        Row(event_id="evt-001", event_timestamp="2026-08-01T14:31:00+00:00", content_id="C1"),
        Row(event_id="evt-002", event_timestamp="2026-08-01T15:00:00+00:00", content_id="C2"),
    ]
    df = spark.createDataFrame(rows)

    unique, duplicates = deduplicate_by_event_id(df)  # log=None for tests

    assert unique.count() == 2
    assert duplicates.count() == 1


@pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PySpark cloudpickle not yet compatible with Python 3.14"
)
def test_unique_events_pass_through(spark: SparkSession):
    """If all event_ids are unique, nothing should be removed."""
    rows = [
        Row(event_id="evt-001", event_timestamp="2026-08-01T14:30:00+00:00", content_id="C1"),
        Row(event_id="evt-002", event_timestamp="2026-08-01T15:00:00+00:00", content_id="C2"),
        Row(event_id="evt-003", event_timestamp="2026-08-01T16:00:00+00:00", content_id="C3"),
    ]
    df = spark.createDataFrame(rows)

    unique, duplicates = deduplicate_by_event_id(df)

    assert unique.count() == 3
    assert duplicates.count() == 0

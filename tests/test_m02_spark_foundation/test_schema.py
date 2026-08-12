"""Tests for Spark schema definitions."""

from pyspark.sql.types import StructType

from etl_playground.m02_spark_foundation.schema import (
    CONTENT_LAUNCH_SCHEMA,
    CONTENT_METADATA_SCHEMA,
    VALID_DEVICE_TYPES,
    VALID_EVENT_TYPES,
    VALID_REGIONS,
    VIEWING_EVENTS_SCHEMA,
)


def test_viewing_events_schema_has_10_columns():
    assert isinstance(VIEWING_EVENTS_SCHEMA, StructType)
    assert len(VIEWING_EVENTS_SCHEMA.fields) == 10


def test_content_metadata_schema_has_6_columns():
    assert isinstance(CONTENT_METADATA_SCHEMA, StructType)
    assert len(CONTENT_METADATA_SCHEMA.fields) == 6


def test_content_launch_schema_has_5_columns():
    assert isinstance(CONTENT_LAUNCH_SCHEMA, StructType)
    assert len(CONTENT_LAUNCH_SCHEMA.fields) == 5


def test_event_id_is_non_nullable():
    event_id_field = VIEWING_EVENTS_SCHEMA.fields[0]
    assert event_id_field.name == "event_id"
    assert not event_id_field.nullable


def test_valid_enums_are_non_empty():
    assert len(VALID_REGIONS) == 4
    assert len(VALID_DEVICE_TYPES) == 5
    assert len(VALID_EVENT_TYPES) == 6
    assert "UK" in VALID_REGIONS
    assert "web" in VALID_DEVICE_TYPES
    assert "play" in VALID_EVENT_TYPES

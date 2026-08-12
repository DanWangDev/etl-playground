"""Read raw CSV/JSON data with explicit schema enforcement.

Demonstrates:
- Schema-on-read: apply known schema at read time
- Malformed record handling: capture, don't silently drop
- Lazy evaluation: no data is actually read until an action is called
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession

from etl_playground.m02_spark_foundation.schema import (
    CONTENT_LAUNCH_SCHEMA,
    CONTENT_METADATA_SCHEMA,
    VIEWING_EVENTS_SCHEMA,
)
from etl_playground.shared.paths import (
    raw_content_launch_path,
    raw_content_metadata_path,
    raw_viewing_events_dir,
)


def read_viewing_events(
    spark: SparkSession,
    ingest_date: date,
) -> DataFrame:
    """Read raw viewing_events CSV with explicit schema.

    Args:
        spark: Active SparkSession.
        ingest_date: The ingestion date partition to read.

    Returns:
        DataFrame with VIEWING_EVENTS_SCHEMA applied.
        NOTE: DataFrame is LAZY — no data read yet.
    """
    input_path = str(raw_viewing_events_dir(ingest_date) / "events.csv")

    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")  # Don't fail on malformed rows; capture them
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .schema(VIEWING_EVENTS_SCHEMA)
        .csv(input_path)
    )

    return df


def read_content_metadata(spark: SparkSession) -> DataFrame:
    """Read content_metadata CSV with explicit schema.

    This is the small reference table — ideal for broadcast joins.
    """
    input_path = str(raw_content_metadata_path())

    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(CONTENT_METADATA_SCHEMA)
        .csv(input_path)
    )

    return df


def read_content_launch(spark: SparkSession) -> DataFrame:
    """Read content_launch CSV with explicit schema."""
    input_path = str(raw_content_launch_path())

    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(CONTENT_LAUNCH_SCHEMA)
        .csv(input_path)
    )

    return df

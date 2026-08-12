"""Field-level validation rules for viewing events.

Each validator returns (valid_df, rejected_df) — valid records and rejected records
with a rejection_reason column explaining why each record was rejected.

This is the "T" in ETL — validate BEFORE transforming to avoid garbage-in-garbage-out.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_playground.m02_spark_foundation.schema import (
    VALID_DEVICE_TYPES,
    VALID_EVENT_TYPES,
    VALID_REGIONS,
    VALID_SUBSCRIPTION_TYPES,
)


def validate_required_fields(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Check that non-nullable columns are not null.

    event_id is the only strictly required field per the schema.
    """
    valid = df.filter(F.col("event_id").isNotNull())
    rejected = df.filter(F.col("event_id").isNull()).withColumn(
        "rejection_reason", F.lit("NULL_EVENT_ID")
    )
    return valid, rejected


def validate_timestamps(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Check that event_timestamp is a valid timestamp.

    Spark's PERMISSIVE mode converts unparseable timestamps to null.
    We capture those as rejected.
    """
    valid = df.filter(F.col("event_timestamp").isNotNull())
    rejected = df.filter(F.col("event_timestamp").isNull()).withColumn(
        "rejection_reason", F.lit("INVALID_TIMESTAMP")
    )
    return valid, rejected


def validate_completion_rate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Check completion_rate is in [0, 1]."""
    valid = df.filter(
        (F.col("completion_rate") >= 0.0) & (F.col("completion_rate") <= 1.0)
    )
    rejected = df.filter(
        (F.col("completion_rate") < 0.0) | (F.col("completion_rate") > 1.0)
    ).withColumn("rejection_reason", F.lit("COMPLETION_RATE_OUT_OF_RANGE"))
    return valid, rejected


def validate_watch_minutes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Check watch_minutes is non-negative and reasonable (< 1000)."""
    valid = df.filter(
        (F.col("watch_minutes") >= 0.0) & (F.col("watch_minutes") <= 1000.0)
    )
    rejected = df.filter(
        (F.col("watch_minutes") < 0.0) | (F.col("watch_minutes") > 1000.0)
    ).withColumn("rejection_reason", F.lit("WATCH_MINUTES_OUT_OF_RANGE"))
    return valid, rejected


def validate_enum_fields(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Check that enum fields contain only valid values."""
    region_ok = F.col("region").isin(list(VALID_REGIONS))
    device_ok = F.col("device_type").isin(list(VALID_DEVICE_TYPES))
    event_ok = F.col("event_type").isin(list(VALID_EVENT_TYPES))
    sub_ok = F.col("subscription_type").isin(list(VALID_SUBSCRIPTION_TYPES))

    all_ok = region_ok & device_ok & event_ok & sub_ok

    valid = df.filter(all_ok)

    # Build detailed rejection reason
    rejected = df.filter(~all_ok).withColumn(
        "rejection_reason",
        F.concat_ws("; ",
            F.when(~region_ok, F.concat(F.lit("INVALID_REGION="), F.col("region"))),
            F.when(~device_ok, F.concat(F.lit("INVALID_DEVICE="), F.col("device_type"))),
            F.when(~event_ok, F.concat(F.lit("INVALID_EVENT_TYPE="), F.col("event_type"))),
            F.when(~sub_ok, F.concat(F.lit("INVALID_SUBSCRIPTION="), F.col("subscription_type"))),
        ),
    )
    return valid, rejected


def validate_all(df: DataFrame, log: "object") -> tuple[DataFrame, DataFrame]:
    """Run all validators in sequence, accumulating rejected rows.

    Returns:
        (clean_df, all_rejected_df) where all_rejected_df has a rejection_reason column.
    """

    total_input = df.count()

    # Apply validators in sequence
    valid, r1 = validate_required_fields(df)
    valid, r2 = validate_timestamps(valid)
    valid, r3 = validate_completion_rate(valid)
    valid, r4 = validate_watch_minutes(valid)
    valid, r5 = validate_enum_fields(valid)

    # Union all rejected
    rejected = r1.unionByName(r2, allowMissingColumns=True)
    rejected = rejected.unionByName(r3, allowMissingColumns=True)
    rejected = rejected.unionByName(r4, allowMissingColumns=True)
    rejected = rejected.unionByName(r5, allowMissingColumns=True)

    total_valid = valid.count()
    total_rejected = rejected.count()

    log.detail(f"Validation: {total_input:,} input → {total_valid:,} valid, "
               f"{total_rejected:,} rejected")

    # Breakdown
    if total_rejected > 0:
        reasons = (
            rejected.groupBy("rejection_reason")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        for row in reasons:
            log.detail(f"  {row['rejection_reason']}: {row['count']:,}")

    return valid, rejected

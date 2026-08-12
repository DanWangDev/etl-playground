"""Normalize timestamps and derive temporal fields.

- Parse event_timestamp to UTC
- Derive event_date (date), year, month, day
- These derived fields become partition keys and dimension attributes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_timestamps(df: DataFrame, log: "ETLLogger") -> DataFrame:
    """Normalize timestamps and derive date fields.

    This is a narrow transformation — no shuffle required.

    Returns DataFrame with added columns: event_date, year, month, day.
    """
    from etl_playground.shared.logging import ETLLogger

    result = (
        df
        .withColumn("event_timestamp", F.to_utc_timestamp("event_timestamp", "UTC"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("day", F.dayofmonth("event_timestamp"))
    )

    log.info("Timestamps normalized → UTC (no shuffle — narrow transformation)")
    log.detail("Derived fields: event_date, year, month, day")

    return result

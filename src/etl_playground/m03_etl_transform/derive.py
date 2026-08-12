"""Derive calculated fields from enriched event data.

- launch_delay_days: difference between actual and planned launch
- watch_hours: watch_minutes / 60.0
- is_completed: completion_rate >= 0.95
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def derive_fields(df: DataFrame, log: "object") -> DataFrame:
    """Calculate derived analytical fields.

    These are narrow transformations — no shuffle required.
    """

    result = (
        df.withColumn(
            "launch_delay_days",
            F.when(
                (F.col("planned_launch_date").isNotNull())
                & (F.col("actual_launch_date").isNotNull())
                & (F.col("actual_launch_date") != ""),
                F.datediff(
                    F.to_date("actual_launch_date"),
                    F.to_date("planned_launch_date"),
                ),
            ).otherwise(None),
        )
        .withColumn(
            "watch_hours",
            F.round(F.col("watch_minutes") / 60.0, 2),
        )
        .withColumn(
            "is_completed",
            F.when(F.col("completion_rate") >= 0.95, True).otherwise(False),
        )
    )

    log.info("Derived fields calculated (narrow transformation)")
    log.detail("  launch_delay_days = datediff(actual, planned)")
    log.detail("  watch_hours = watch_minutes / 60.0")
    log.detail("  is_completed = completion_rate >= 0.95")

    return result

"""Aggregate viewing events to analytical grains.

Produces daily aggregates: content + region + device + event_date grain.
This is the foundation for the fact_viewing warehouse table.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_daily(df: DataFrame, log: "object") -> DataFrame:
    """Aggregate viewing events to the daily analytical grain.

    Grain: one row per content_id + region + device_type + event_date.

    This is a SHUFFLE operation (GROUP BY requires data redistribution).
    """

    log.info("Aggregating to daily grain (content + region + device + date)...")
    log.spark("SHUFFLE: GROUP BY requires data redistribution by aggregation keys")

    agg = (
        df
        .groupBy("content_id", "region", "device_type", "event_date")
        .agg(
            F.count("*").alias("views"),
            F.sum("watch_minutes").alias("total_watch_minutes"),
            F.round(F.sum("watch_minutes") / 60.0, 2).alias("total_watch_hours"),
            F.sum(F.when(F.col("is_completed"), 1).otherwise(0)).alias("completed_views"),
            F.round(F.avg("completion_rate"), 4).alias("avg_completion_rate"),
        )
        .withColumn("event_date", F.to_date("event_date"))
    )

    log.success(f"Aggregation complete: {agg.count():,} rows "
                f"(grain: content + region + device + date)")
    return agg

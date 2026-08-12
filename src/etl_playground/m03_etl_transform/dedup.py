"""Deduplication logic — remove duplicate events by event_id.

Strategy: Row number window partitioned by event_id, ordered by event_timestamp DESC.
Keep the latest event for each event_id; older duplicates go to rejected.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_by_event_id(
    df: DataFrame, log: "object | None" = None
) -> tuple[DataFrame, DataFrame]:
    """Deduplicate on event_id, keeping the latest timestamp.

    Uses a window function: row_number() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC).
    Row 1 = latest → keep. Row > 1 = older duplicate → reject.

    IMPORTANT: This causes a SHUFFLE because window functions with PARTITION BY
    require data to be redistributed by the partition key.

    Returns:
        (unique_df, duplicates_df) — unique records and rejected duplicates.
    """

    input_count = df.count()

    window_spec = Window.partitionBy("event_id").orderBy(F.desc("event_timestamp"))

    ranked = df.withColumn("_row_num", F.row_number().over(window_spec))

    unique = ranked.filter(F.col("_row_num") == 1).drop("_row_num")
    duplicates = (
        ranked.filter(F.col("_row_num") > 1)
        .drop("_row_num")
        .withColumn("rejection_reason", F.lit("DUPLICATE_EVENT_ID"))
    )

    output_count = unique.count()
    dup_count = duplicates.count()

    if log:
        log.info(
            f"Dedup: {input_count:,} input → {output_count:,} unique "
            f"({dup_count:,} duplicates removed)"
        )
        log.spark(
            "SHUFFLE triggered by: row_number() PARTITION BY event_id "
            "(window function requires data redistribution)"
        )

    return unique, duplicates

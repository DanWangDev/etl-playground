"""Enrich viewing events with content metadata and launch data.

- Broadcast join: content_metadata is small (1,000 rows) → fits in memory
- Regular join: content_launch is also small but demonstrates sort-merge behavior
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_with_metadata(
    df: DataFrame,
    metadata_df: DataFrame,
    log: "object",
) -> DataFrame:
    """Join viewing events with content metadata using BROADCAST JOIN.

    content_metadata is small (~1,000 rows, <1 MB) → ideal for broadcast.
    Broadcasting avoids a shuffle of the large viewing_events table.

    Returns enriched DataFrame with title, genre, studio, release_date, content_type.
    """

    log.info("Enriching with content_metadata (BROADCAST JOIN)...")
    log.spark(
        f"Left: {df.count():,} viewing events | Right: {metadata_df.count():,} content items"
    )
    log.spark("Strategy: BROADCAST — content_metadata fits in executor memory (< 1 MB)")
    log.spark("This avoids shuffling the large viewing_events table")

    enriched = df.join(
        F.broadcast(metadata_df),
        on="content_id",
        how="left",
    )

    # Check for unmatched joins
    null_titles = enriched.filter(F.col("title").isNull()).count()
    if null_titles > 0:
        log.warn(f"{null_titles:,} events have no matching content_id (unmatched join)")

    log.success(f"Join complete: {enriched.count():,} rows")
    return enriched


def enrich_with_launch(
    df: DataFrame,
    launch_df: DataFrame,
    log: "object",
) -> DataFrame:
    """Join with content_launch data.

    Uses a regular sort-merge join (content_launch is 4,000 rows — small but
    we use the default join to demonstrate non-broadcast behavior).
    """

    log.info("Enriching with content_launch (default sort-merge join)...")
    log.spark(f"Join key: content_id + region | Right side: {launch_df.count():,} rows")
    log.spark("SHUFFLE: Both sides redistributed by join keys for sort-merge join")

    enriched = df.join(
        launch_df,
        on=["content_id", "region"],
        how="left",
    )

    log.success(f"Join complete: {enriched.count():,} rows")
    return enriched

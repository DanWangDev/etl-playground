"""Write curated datasets as partitioned Parquet.

Key design decisions demonstrated:
- Partition keys: event_date + region (align with typical query filters)
- Anti-pattern: customer_id or content_id (high cardinality → small-file problem)
- Repartition before write to control output file count
"""

from pyspark.sql import DataFrame


def write_partitioned_parquet(
    df: DataFrame,
    output_base: str,
    partition_keys: list[str] | None = None,
    log=None,
) -> str:
    """Write DataFrame as partitioned Parquet.

    Args:
        df: DataFrame to write.
        output_base: Base output directory.
        partition_keys: Columns to partition by (default: event_date, region).
        log: Logger for progress reporting.

    Returns:
        The output path written to.
    """
    if partition_keys is None:
        partition_keys = ["event_date", "region"]

    if log:
        log.info(f"Writing partitioned Parquet to: {output_base}")
        log.spark(f"Partition keys: {', '.join(partition_keys)}")
        log.detail(f"Input rows: {df.count():,}")

    # Repartition to control output file count
    # Use a moderate number to avoid the small-file problem
    num_partitions = max(1, df.rdd.getNumPartitions())
    if log:
        log.spark(
            f"Output partitions: {num_partitions} "
            f"(coalesced from {df.rdd.getNumPartitions()})"
        )

    (
        df.repartition(*partition_keys)
        .write.mode("overwrite")
        .partitionBy(*partition_keys)
        .parquet(output_base)
    )

    if log:
        log.success(f"Parquet written: {output_base}")

    return output_base


def list_partitions(output_base: str, log=None) -> list[str]:
    """List the partition directories created.

    Demonstrates the partition structure: event_date=YYYY-MM-DD/region=XX/
    """
    from pathlib import Path

    path = Path(output_base)
    if not path.exists():
        log.warn(f"Output path does not exist: {output_base}")
        return []

    partitions = []
    for p in sorted(path.rglob("*.parquet")):
        partitions.append(str(p.relative_to(path)))

    log.info(f"Partition files found: {len(partitions)}")
    for p in partitions[:10]:  # Show first 10
        log.detail(f"  {p}")
    if len(partitions) > 10:
        log.detail(f"  ... and {len(partitions) - 10} more")

    return partitions

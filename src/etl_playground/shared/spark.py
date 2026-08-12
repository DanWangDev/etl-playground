"""Spark session factory.

Provides a local PySpark session configured to match Glue-like defaults.
All DataFrame API code is identical to what would run on AWS Glue.
"""

import os
import tempfile
from pathlib import Path

# ── Windows Hadoop compatibility ──
# Must be set BEFORE pyspark is imported (JVM startup reads these)
if "HADOOP_HOME" not in os.environ:
    _hadoop_home = Path(tempfile.gettempdir()) / "hadoop_temp"
    _hadoop_home.mkdir(parents=True, exist_ok=True)
    _bin_dir = _hadoop_home / "bin"
    _bin_dir.mkdir(exist_ok=True)
    os.environ["HADOOP_HOME"] = str(_hadoop_home)
    os.environ["hadoop.home.dir"] = str(_hadoop_home)
    os.environ["PATH"] = str(_bin_dir) + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

from etl_playground.shared.config import get_settings


def create_spark_session(app_name: str = "etl-playground") -> SparkSession:
    """Create a local SparkSession with Glue-compatible settings.

    Returns a configured SparkSession in local[*] mode.
    The same DataFrame code runs unchanged on AWS Glue.
    """
    settings = get_settings()

    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        # Logging-friendly: don't flood console with Spark internal logs
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_or_create_spark(app_name: str = "etl-playground") -> SparkSession:
    """Get existing SparkSession or create a new one."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = create_spark_session(app_name)
    return spark

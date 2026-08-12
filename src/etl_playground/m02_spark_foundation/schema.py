"""Explicit PySpark schemas for all three datasets.

Defining schemas explicitly (rather than inferring from CSV) is a production best practice:
- Catches schema drift at read time
- Documents the expected data contract
- Avoids inference overhead on large files
"""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── viewing_events schema ──
VIEWING_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("region", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("watch_minutes", DoubleType(), True),
        StructField("completion_rate", DoubleType(), True),
        StructField("subscription_type", StringType(), True),
    ]
)

# ── content_metadata schema ──
CONTENT_METADATA_SCHEMA = StructType(
    [
        StructField("content_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("content_type", StringType(), True),
    ]
)

# ── content_launch schema ──
CONTENT_LAUNCH_SCHEMA = StructType(
    [
        StructField("content_id", StringType(), False),
        StructField("region", StringType(), True),
        StructField("planned_launch_date", StringType(), True),
        StructField("actual_launch_date", StringType(), True),
        StructField("launch_status", StringType(), True),
    ]
)

# ── Valid enums (used by validators) ──
VALID_REGIONS = {"UK", "US", "DE", "JP"}
VALID_DEVICE_TYPES = {"web", "mobile", "tv", "console", "tablet"}
VALID_EVENT_TYPES = {"play", "pause", "complete", "launch-view", "skip", "rewind"}
VALID_SUBSCRIPTION_TYPES = {"free", "basic", "premium", "family"}
VALID_LAUNCH_STATUSES = {"on_time", "delayed", "cancelled"}

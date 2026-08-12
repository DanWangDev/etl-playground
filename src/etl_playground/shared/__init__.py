"""Shared utilities for ETL Playground.

Re-exports the most commonly used shared components for convenience.
"""

from etl_playground.shared.config import Settings, get_settings
from etl_playground.shared.duckdb import create_duckdb_connection, query_parquet
from etl_playground.shared.logging import (
    PipelineRun,
    StageResult,
    etl_context,
    get_logger,
    progress_bar,
    spinner_context,
)
from etl_playground.shared.paths import ensure_all_dirs
from etl_playground.shared.spark import create_spark_session, get_or_create_spark

__all__ = [
    # Config
    "Settings",
    "get_settings",
    # Spark
    "create_spark_session",
    "get_or_create_spark",
    # DuckDB
    "create_duckdb_connection",
    "query_parquet",
    # Logging
    "get_logger",
    "PipelineRun",
    "StageResult",
    "etl_context",
    "spinner_context",
    "progress_bar",
    # Paths
    "ensure_all_dirs",
]

"""Configuration via environment variables with sensible defaults.

ETL Playground settings — loaded from .env file with Pydantic validation.
"""

from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    """ETL Playground configuration from environment/.env."""

    # ── Project root ──
    # config.py is at src/etl_playground/shared/config.py → root is 4 levels up
    project_root: Path = Path(__file__).resolve().parent.parent.parent.parent

    # ── Data Generator ──
    data_scale: int = 500_000
    random_seed: int = 42
    num_content_items: int = 1_000
    num_regions: int = 4
    regions: str = "UK,US,DE,JP"
    start_date: str = "2026-07-14"

    # ── Data Quality Injection ──
    duplicate_rate: float = 0.0025
    malformed_rate: float = 0.0007
    late_event_rate: float = 0.0042

    # ── Spark Configuration ──
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_shuffle_partitions: int = 200

    # ── Paths (relative to project root) ──
    data_raw_dir: str = "data/raw"
    data_curated_dir: str = "data/curated"
    data_rejected_dir: str = "data/rejected"
    runs_dir: str = "runs"
    schema_dir: str = "schema"
    warehouse_path: str = "data/warehouse.duckdb"

    # ── Logging ──
    log_level: Literal["DEBUG", "INFO", "WARN"] = "INFO"
    log_json: bool = True

    @property
    def region_list(self) -> list[str]:
        return [r.strip() for r in self.regions.split(",")]

    @property
    def raw_base(self) -> Path:
        return self.project_root / self.data_raw_dir

    @property
    def curated_base(self) -> Path:
        return self.project_root / self.data_curated_dir

    @property
    def rejected_base(self) -> Path:
        return self.project_root / self.data_rejected_dir

    @property
    def runs_base(self) -> Path:
        return self.project_root / self.runs_dir

    @property
    def schema_base(self) -> Path:
        return self.project_root / self.schema_dir

    @property
    def warehouse_db_path(self) -> Path:
        return self.project_root / self.warehouse_path

    model_config = {"env_prefix": "", "case_sensitive": False}


# Singleton
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the global settings instance (lazy init)."""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.runs_base.mkdir(parents=True, exist_ok=True)
    return _settings

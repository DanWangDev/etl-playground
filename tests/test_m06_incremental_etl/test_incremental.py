"""Tests for incremental processing logic."""

from datetime import date
from pathlib import Path

from etl_playground.m06_incremental_etl.incremental import find_new_partitions


def test_find_new_partitions_empty_dirs(tmp_path: Path):
    """With empty directories, no partitions should be found."""
    raw_base = tmp_path / "raw"
    curated_base = tmp_path / "curated"
    raw_base.mkdir()
    curated_base.mkdir()

    new, reprocess = find_new_partitions(raw_base, curated_base)
    assert len(new) == 0
    assert len(reprocess) == 0


def test_find_new_partitions_detects_unprocessed(tmp_path: Path):
    """Raw partitions without curated equivalents should be flagged as new."""
    raw_base = tmp_path / "raw"
    curated_base = tmp_path / "curated"
    raw_base.mkdir()
    curated_base.mkdir()

    # Create a raw partition
    (raw_base / "ingest_date=2026-08-12").mkdir()

    new, reprocess = find_new_partitions(raw_base, curated_base)

    assert date(2026, 8, 12) in new

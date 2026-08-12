"""Tests for backfill date parsing."""

from datetime import date

import pytest

from etl_playground.m06_incremental_etl.backfill import parse_date_range


def test_parse_single_date():
    """A single date should return a list with one date."""
    dates = parse_date_range("2026-08-01", "2026-08-01")
    assert dates == [date(2026, 8, 1)]


def test_parse_date_range():
    """A date range should return all dates inclusive."""
    dates = parse_date_range("2026-08-01", "2026-08-03")
    assert dates == [
        date(2026, 8, 1),
        date(2026, 8, 2),
        date(2026, 8, 3),
    ]


def test_parse_rejects_inverted_range():
    """Start date after end date should raise ValueError."""
    with pytest.raises(ValueError):
        parse_date_range("2026-08-05", "2026-08-01")


def test_parse_end_defaults_to_today():
    """When end is None, it should default to today."""
    dates = parse_date_range("2026-08-10", None)
    assert len(dates) >= 1
    assert dates[0] == date(2026, 8, 10)


def test_parse_all_returns_empty_list():
    """'all' keyword returns empty list (caller scans directory)."""
    dates = parse_date_range("all")
    assert dates == []

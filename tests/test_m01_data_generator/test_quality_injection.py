"""Tests for data quality injection in the generator.

These tests verify that the generator produces events with the expected
characteristics. They use default settings to avoid cached-singleton issues.
"""

from etl_playground.m01_data_generator.generators import generate_content_metadata


class NoopLog:
    def info(self, *a, **kw):
        pass

    def success(self, *a, **kw):
        pass

    def detail(self, *a, **kw):
        pass


def test_content_metadata_has_diverse_genres():
    """Generated content should span multiple genres, not just one."""
    items = generate_content_metadata(NoopLog())

    genres = {item["genre"] for item in items}
    # With 1000 items, we should see at least 5 different genres
    assert len(genres) >= 5, f"Only found {len(genres)} genres: {genres}"


def test_content_metadata_has_diverse_studios():
    """Generated content should come from multiple studios."""
    items = generate_content_metadata(NoopLog())

    studios = {item["studio"] for item in items}
    # Should have at least 3 different studios
    assert len(studios) >= 3, f"Only found {len(studios)} studios: {studios}"


def test_content_metadata_has_mix_of_types():
    """Generated content should include multiple content types."""
    from etl_playground.m01_data_generator.generators import CONTENT_TYPES

    items = generate_content_metadata(NoopLog())

    types_found = {item["content_type"] for item in items}
    # Should have at least 3 of the 5 content types
    assert len(types_found) >= 3, (
        f"Only found {len(types_found)} content types: {types_found}, "
        f"expected >=3 from {CONTENT_TYPES}"
    )

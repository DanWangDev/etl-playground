"""Tests for the data generator module."""

from etl_playground.m01_data_generator.generators import generate_content_metadata


def test_generate_content_metadata_has_correct_count():
    """Content metadata should generate NUM_CONTENT_ITEMS rows."""

    # We use a no-op logger for tests
    class NoopLog:
        def info(self, *a, **kw):
            pass

        def success(self, *a, **kw):
            pass

        def detail(self, *a, **kw):
            pass

    items = generate_content_metadata(NoopLog())
    assert len(items) == 1000  # Default from settings
    # Verify required fields
    for item in items:
        assert "content_id" in item
        assert "title" in item
        assert "genre" in item
        assert "studio" in item
        assert "release_date" in item
        assert "content_type" in item
        assert item["content_id"].startswith("CONT-")

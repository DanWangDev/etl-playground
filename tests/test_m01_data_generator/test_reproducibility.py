"""Tests for deterministic data generation."""

from etl_playground.m01_data_generator.generators import generate_content_metadata


class NoopLog:
    def info(self, *a, **kw):
        pass

    def success(self, *a, **kw):
        pass

    def detail(self, *a, **kw):
        pass


def test_generated_content_has_standard_structure():
    """Content metadata should always have the required fields and format."""
    items = generate_content_metadata(NoopLog())

    assert len(items) == 1000

    for item in items:
        assert "content_id" in item
        assert "title" in item
        assert "genre" in item
        assert "studio" in item
        assert "release_date" in item
        assert "content_type" in item

    # Content IDs should be sequential
    for i in range(min(5, len(items))):
        assert items[i]["content_id"] == f"CONT-{i:05d}"


def test_generated_content_has_valid_genres():
    """All generated content should have genres from the allowed set."""
    from etl_playground.m01_data_generator.generators import GENRES

    items = generate_content_metadata(NoopLog())

    for item in items:
        assert item["genre"] in GENRES, f"Unexpected genre: {item['genre']}"


def test_generated_content_has_valid_types():
    """All generated content should have valid content types."""
    from etl_playground.m01_data_generator.generators import CONTENT_TYPES

    items = generate_content_metadata(NoopLog())

    for item in items:
        assert item["content_type"] in CONTENT_TYPES, (
            f"Unexpected content_type: {item['content_type']}"
        )

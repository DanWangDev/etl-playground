"""Tests for deterministic data generation."""

import random

from etl_playground.m01_data_generator.generators import generate_content_metadata


class NoopLog:
    def info(self, *a, **kw):
        pass

    def success(self, *a, **kw):
        pass

    def detail(self, *a, **kw):
        pass


def test_same_seed_produces_same_output():
    """Same random seed should produce identical content metadata."""
    import os

    os.environ["RANDOM_SEED"] = "42"
    random.seed(42)

    items1 = generate_content_metadata(NoopLog())

    random.seed(42)
    items2 = generate_content_metadata(NoopLog())

    # Both runs should produce the same content_ids in the same order
    ids1 = [item["content_id"] for item in items1]
    ids2 = [item["content_id"] for item in items2]
    assert ids1 == ids2

    del os.environ["RANDOM_SEED"]


def test_different_seed_produces_different_output():
    """Different random seeds should produce different content."""
    import os

    os.environ["RANDOM_SEED"] = "42"
    random.seed(42)
    items1 = generate_content_metadata(NoopLog())

    os.environ["RANDOM_SEED"] = "999"
    random.seed(999)
    items2 = generate_content_metadata(NoopLog())

    titles1 = [item["title"] for item in items1]
    titles2 = [item["title"] for item in items2]
    assert titles1 != titles2

    del os.environ["RANDOM_SEED"]

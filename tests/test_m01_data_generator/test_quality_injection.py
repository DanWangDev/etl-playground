"""Tests for data quality injection in the generator."""

import random

from etl_playground.m01_data_generator.generators import generate_viewing_events


class NoopLog:
    def info(self, *a, **kw): pass
    def success(self, *a, **kw): pass
    def detail(self, *a, **kw): pass


def test_duplicates_are_injected():
    """Generated events should include approximate duplicate_rate × scale duplicates."""
    content_items = [
        {"content_id": f"CONT-{i:05d}", "title": f"Title {i}", "genre": "Action",
         "release_date": "2025-01-01"}
        for i in range(10)
    ]
    launch_items = []

    # Temporarily override settings
    import os
    os.environ["DATA_SCALE"] = "2000"
    os.environ["DUPLICATE_RATE"] = "0.05"  # 5% → ~100 duplicates
    os.environ["MALFORMED_RATE"] = "0.0"
    os.environ["LATE_EVENT_RATE"] = "0.0"
    os.environ["RANDOM_SEED"] = "42"

    random.seed(42)

    events = generate_viewing_events(NoopLog(), content_items, launch_items)

    # We should have more than base count (2000) due to duplicates
    assert len(events) > 2000
    # Should not exceed base + 5% too much
    assert len(events) < 2200

    # Clean up
    del os.environ["DATA_SCALE"]
    del os.environ["DUPLICATE_RATE"]
    del os.environ["MALFORMED_RATE"]
    del os.environ["LATE_EVENT_RATE"]


def test_malformed_events_have_invalid_values():
    """Malformed events should contain out-of-range completion_rates."""
    content_items = [
        {"content_id": f"CONT-{i:05d}", "title": f"Title {i}", "genre": "Drama",
         "release_date": "2025-06-01"}
        for i in range(5)
    ]
    launch_items = []

    import os
    os.environ["DATA_SCALE"] = "1000"
    os.environ["DUPLICATE_RATE"] = "0.0"
    os.environ["MALFORMED_RATE"] = "0.05"  # 5% → ~50 malformed
    os.environ["LATE_EVENT_RATE"] = "0.0"
    os.environ["RANDOM_SEED"] = "99"

    random.seed(99)

    events = generate_viewing_events(NoopLog(), content_items, launch_items)

    # Find malformed events (completion_rate outside [0, 1])
    malformed = [
        e for e in events
        if e["completion_rate"] < 0 or e["completion_rate"] > 1
    ]
    assert len(malformed) > 0, "Should have injected malformed events"

    # Clean up
    for key in ["DATA_SCALE", "DUPLICATE_RATE", "MALFORMED_RATE", "LATE_EVENT_RATE"]:
        os.environ.pop(key, None)

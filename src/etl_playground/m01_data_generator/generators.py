"""Synthetic data generators for content analytics events.

Generates three datasets:
1. content_metadata — reference table (content catalog)
2. content_launch — launch schedule per content + region
3. viewing_events — the main event stream with injected data quality issues

Uses Faker for realistic-looking IDs and deterministic random seed for reproducibility.
"""

import csv
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from faker import Faker

from etl_playground.shared.config import get_settings
from etl_playground.shared.logging import get_logger
from etl_playground.shared.paths import (
    raw_content_launch_path,
    raw_content_metadata_path,
    raw_viewing_events_dir,
)

# ── Constants ──

DEVICE_TYPES = ["web", "mobile", "tv", "console", "tablet"]
DEVICE_WEIGHTS = [0.30, 0.30, 0.20, 0.15, 0.05]

EVENT_TYPES = ["play", "pause", "complete", "launch-view", "skip", "rewind"]
EVENT_WEIGHTS = [0.40, 0.20, 0.16, 0.14, 0.06, 0.04]

SUBSCRIPTION_TYPES = ["free", "basic", "premium", "family"]
SUB_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

GENRES = [
    "Action", "Comedy", "Drama", "Documentary", "Horror",
    "Sci-Fi", "Romance", "Thriller", "Animation", "Kids",
    "Reality", "Sports", "News", "Music", "Fantasy",
]

CONTENT_TYPES = ["movie", "series", "documentary", "special", "short"]
CONTENT_TYPE_WEIGHTS = [0.35, 0.35, 0.15, 0.10, 0.05]

STUDIOS = [
    "Prime Studios", "Global Films", "Indie Productions",
    "StreamFirst", "Classic Media", "NextGen Content",
    "Horizon Entertainment", "Blue Sky Productions", "Urban Media Co",
]


def generate_content_metadata(log) -> list[dict]:
    """Generate the content_metadata reference table.

    Returns a list of dicts: content_id, title, genre, studio, release_date, content_type.
    """
    settings = get_settings()
    fake = Faker()
    Faker.seed(settings.random_seed)
    random.seed(settings.random_seed)

    n = settings.num_content_items
    items = []

    log.info(f"Generating {n:,} content items...")

    for i in range(n):
        content_id = f"CONT-{i:05d}"
        genre = random.choices(GENRES, k=1)[0]
        content_type = random.choices(CONTENT_TYPES, weights=CONTENT_TYPE_WEIGHTS, k=1)[0]
        studio = random.choice(STUDIOS)

        # Title: realistic-looking
        if content_type == "series":
            title = f"{fake.word().title()} {fake.word().title()} S{random.randint(1, 8):02d}"
        elif content_type == "movie":
            title = f"The {fake.word().title()} {random.choice(['Chronicles', 'Effect', 'Legacy', 'Code', 'Protocol', 'Edge', 'Reckoning', 'Horizon'])}"
        elif content_type == "documentary":
            title = f"{fake.word().title()}: {random.choice(['Untold', 'Inside', 'The Truth About', 'Rise of', 'Secrets of'])} {fake.word().title()}"
        else:
            title = f"{fake.word().title()} {random.choice(['Presents', 'Special', 'Live', 'Event'])}"

        # Release date: 2018-2026
        days_ago = random.randint(100, 3000)
        release_date = date.today() - timedelta(days=days_ago)

        items.append({
            "content_id": content_id,
            "title": title,
            "genre": genre,
            "studio": studio,
            "release_date": release_date.isoformat(),
            "content_type": content_type,
        })

    log.success(f"{len(items):,} content items generated")
    return items


def generate_content_launch(log: "object", content_items: list[dict]) -> list[dict]:
    """Generate content_launch records — one per content per region.

    Returns a list of dicts: content_id, region, planned_launch_date, actual_launch_date, launch_status.
    """
    settings = get_settings()
    random.seed(settings.random_seed + 1)

    regions = settings.region_list
    launches = []

    log.info(f"Generating launch records ({len(content_items)} × {len(regions)} regions)...")

    for item in content_items:
        content_id = item["content_id"]
        release_date = date.fromisoformat(item["release_date"])

        for region in regions:
            # Planned launch: around release date, with regional stagger
            stagger_days = random.randint(0, 30)
            planned = release_date + timedelta(days=stagger_days)

            # Actual launch: sometimes delayed
            delay_days = 0
            status = "on_time"
            if random.random() < 0.15:  # 15% delayed
                delay_days = random.randint(1, 60)
                status = "delayed"
            if random.random() < 0.02:  # 2% cancelled
                status = "cancelled"
                delay_days = 0

            actual = planned + timedelta(days=delay_days) if status != "cancelled" else None

            launches.append({
                "content_id": content_id,
                "region": region,
                "planned_launch_date": planned.isoformat(),
                "actual_launch_date": actual.isoformat() if actual else "",
                "launch_status": status,
            })

    log.success(f"{len(launches):,} launch records generated ({len(regions)} regions × {len(content_items)} content)")
    return launches


def generate_viewing_events(
    log: "object",
    content_items: list[dict],
    launch_items: list[dict],
) -> list[dict]:
    """Generate the main viewing_events dataset with data quality injection.

    Builds events with realistic distributions:
    - Content popularity follows a power-law (a few hits, long tail)
    - Region distribution is uniform
    - Device types and event types follow platform-typical ratios
    - Data quality issues are intentionally injected

    Returns a list of dicts matching the viewing_events schema.
    """
    settings = get_settings()
    random.seed(settings.random_seed + 2)
    Faker.seed(settings.random_seed + 2)
    fake = Faker()

    n = settings.data_scale
    regions = settings.region_list
    today = date.today()
    start_date = date.fromisoformat(settings.start_date)
    days_span = (today - start_date).days

    # Build content popularity: power-law distribution
    content_popularity = {}
    for i, item in enumerate(content_items):
        # Zipf-like: top content gets more weight
        rank = i + 1
        weight = 1.0 / (rank ** 0.8)  # Less steep than pure Zipf
        content_popularity[item["content_id"]] = weight

    content_ids = [c["content_id"] for c in content_items]
    content_weights = [content_popularity[cid] for cid in content_ids]

    # For launch join — build lookup keyed by (content_id, region)
    launch_lookup = {}
    for l in launch_items:
        launch_lookup[(l["content_id"], l["region"])] = l

    # Data quality counters
    duplicates_planned = int(n * settings.duplicate_rate)
    malformed_planned = int(n * settings.malformed_rate)
    late_planned = int(n * settings.late_event_rate)

    log.info(f"Generating {n:,} viewing events across {days_span} days...")
    log.detail(f"Date range: {start_date} → {today}")
    log.detail(f"Regions: {', '.join(regions)}")
    log.detail(f"Injected issues: {duplicates_planned:,} duplicates, "
               f"{malformed_planned:,} malformed, {late_planned:,} late events")

    events = []
    dup_pool: list[dict] = []  # Pool for creating duplicates
    log_interval = max(1, n // 10)  # Log progress every 10%

    for i in range(n):
        # Content: weighted by popularity
        content_id = random.choices(content_ids, weights=content_weights, k=1)[0]

        # Region: uniform
        region = random.choice(regions)

        # Event timestamp: uniform across date range, with time-of-day pattern
        event_day_offset = random.randint(0, days_span - 1)
        event_date_val = start_date + timedelta(days=event_day_offset)
        # More viewing in evening hours
        hour = random.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 5, 5, 4, 4, 4, 5, 6, 7, 8, 8, 7, 5, 3, 2],
            k=1,
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_ts = datetime(
            event_date_val.year, event_date_val.month, event_date_val.day,
            hour, minute, second, tzinfo=timezone.utc
        )

        # Device type
        device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]

        # Event type
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

        # Watch minutes and completion rate: correlate with event type
        if event_type == "complete":
            watch_minutes = round(random.uniform(30, 180), 1)
            completion_rate = round(random.uniform(0.95, 1.0), 2)
        elif event_type == "play":
            watch_minutes = round(random.uniform(5, 120), 1)
            completion_rate = round(random.uniform(0.1, 0.8), 2)
        elif event_type == "pause":
            watch_minutes = round(random.uniform(3, 60), 1)
            completion_rate = round(random.uniform(0.2, 0.7), 2)
        elif event_type == "skip":
            watch_minutes = round(random.uniform(0.5, 10), 1)
            completion_rate = round(random.uniform(0.0, 0.1), 2)
        elif event_type == "rewind":
            watch_minutes = round(random.uniform(1, 20), 1)
            completion_rate = round(random.uniform(0.3, 0.8), 2)
        else:  # launch-view
            watch_minutes = round(random.uniform(0.1, 5), 1)
            completion_rate = 0.0

        # Subscription type
        subscription_type = random.choices(SUBSCRIPTION_TYPES, weights=SUB_WEIGHTS, k=1)[0]

        # Customer
        customer_id = f"CUST-{random.randint(0, 99999):05d}"

        event = {
            "event_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "content_id": content_id,
            "event_timestamp": event_ts.isoformat(),
            "region": region,
            "device_type": device_type,
            "event_type": event_type,
            "watch_minutes": watch_minutes,
            "completion_rate": completion_rate,
            "subscription_type": subscription_type,
        }

        events.append(event)

        # Feed the duplicate pool (keep last ~N for duplication)
        if len(dup_pool) < max(duplicates_planned * 2, 100):
            dup_pool.append(event)
        elif random.random() < 0.01:
            dup_pool[random.randint(0, len(dup_pool) - 1)] = event

        if i % log_interval == 0 and i > 0:
            log.detail(f"  ... {i:,}/{n:,} events generated")

    # ── Inject duplicates (copy event_id from existing event) ──
    if duplicates_planned > 0 and dup_pool:
        log.detail(f"Injecting {duplicates_planned:,} duplicate events...")
        for _ in range(duplicates_planned):
            original = random.choice(dup_pool)
            dup = dict(original)
            dup["event_timestamp"] = (
                datetime.fromisoformat(original["event_timestamp"]) + timedelta(milliseconds=random.randint(1, 500))
            ).isoformat()
            events.append(dup)

    # ── Inject malformed rows ──
    if malformed_planned > 0:
        log.detail(f"Injecting {malformed_planned:,} malformed events...")
        for _ in range(malformed_planned):
            bad = {
                "event_id": str(uuid.uuid4()),
                "customer_id": random.choice(content_ids),  # Wrong: using content_id as customer_id
                "content_id": random.choice(content_ids),
                "event_timestamp": "NOT-A-TIMESTAMP",  # Malformed
                "region": random.choice(["XX", "YY", ""]),  # Invalid region
                "device_type": random.choice(DEVICE_TYPES),
                "event_type": "unknown_event_type",  # Invalid enum
                "watch_minutes": -1.0 if random.random() < 0.5 else 99999.0,  # Out of range
                "completion_rate": random.choice([-0.5, 1.5, 2.0]),  # Out of [0,1]
                "subscription_type": "ultra-premium",  # Invalid subscription
            }
            events.append(bad)

    # ── Inject late-arriving events (timestamp far in the past, simulating late arrival) ──
    if late_planned > 0:
        log.detail(f"Injecting {late_planned:,} late-arriving events (backdated)...")
        for _ in range(late_planned):
            if not events:
                break
            template = random.choice(events[:n])  # Pick from clean events
            late = dict(template)
            late["event_id"] = str(uuid.uuid4())
            # Set timestamp 3-7 days in the past (simulating late arrival)
            late_date = today - timedelta(days=random.randint(3, 7))
            late["event_timestamp"] = datetime(
                late_date.year, late_date.month, late_date.day,
                random.randint(0, 23), random.randint(0, 59), random.randint(0, 59),
                tzinfo=timezone.utc,
            ).isoformat()
            events.append(late)

    # Shuffle to mix injected issues throughout the dataset
    random.shuffle(events)

    log.success(f"{len(events):,} total events generated "
                f"(base: {n:,} + duplicates: {duplicates_planned:,} "
                f"+ malformed: {malformed_planned:,} + late: {late_planned:,})")

    # Breakdown by dimension
    log.detail("Event distribution:")
    for region in regions:
        count = sum(1 for e in events[:100000] if e["region"] == region)
        log.detail(f"  {region}: ~{count * len(events) // 100000:,} (sampled)")

    return events


def write_content_metadata_csv(items: list[dict]) -> None:
    """Write content_metadata to CSV."""
    path = raw_content_metadata_path()
    fieldnames = ["content_id", "title", "genre", "studio", "release_date", "content_type"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items)


def write_content_launch_csv(items: list[dict]) -> None:
    """Write content_launch to CSV."""
    path = raw_content_launch_path()
    fieldnames = ["content_id", "region", "planned_launch_date", "actual_launch_date", "launch_status"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items)


def write_viewing_events_csv(events: list[dict], ingest_date: date) -> None:
    """Write viewing_events to CSV in the raw zone, partitioned by ingest_date."""
    dir_path = raw_viewing_events_dir(ingest_date)
    dir_path.mkdir(parents=True, exist_ok=True)
    path = dir_path / "events.csv"

    fieldnames = [
        "event_id", "customer_id", "content_id", "event_timestamp",
        "region", "device_type", "event_type", "watch_minutes",
        "completion_rate", "subscription_type",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

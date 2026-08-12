"""Module 01: Synthetic Data Generator.

Generates three datasets for the ETL pipeline:
1. content_metadata — reference data (1,000 content items)
2. content_launch — launch schedules per content + region
3. viewing_events — the main event stream (500K–20M rows)

Design principles:
- Deterministic: same seed → same output (reproducible benchmarks)
- Realistic: follows content-platform distributions (popularity, regions, device mix)
- Data-quality aware: injects duplicates, malformed rows, and late events intentionally
  so the ETL pipeline has something to handle
"""

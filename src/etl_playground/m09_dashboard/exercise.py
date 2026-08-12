"""Module 09 Exercise: Launch the Streamlit Dashboard.

Usage:
    uv run python -m etl_playground.m09_dashboard.exercise
    # or directly:
    uv run streamlit run src/etl_playground/m09_dashboard/app.py
"""

import subprocess
import sys
from pathlib import Path

from etl_playground.shared.logging import get_logger


def main() -> None:
    log = get_logger("M09_Dashboard")
    log.header("ETL Playground — Streamlit Dashboard (≈ QuickSight)")

    log.info("Launching Streamlit dashboard...")
    log.info("The dashboard demonstrates analytical consumption of the ETL pipeline.")
    log.info("")
    log.info("5 Visuals:")
    log.info("  1. KPI Cards: Total Watch Hours, Views, Avg Completion, Unique Content")
    log.info("  2. Weekly Engagement Trend (bar chart)")
    log.info("  3. Completion Rate by Device Type (bar chart)")
    log.info("  4. Top 10 Titles by Watch Hours (table)")
    log.info("  5. Launch Success Rate by Region (table)")
    log.info("")
    log.info("Filters: Region (multi-select), Date Range (date picker)")
    log.info("")

    app_path = Path(__file__).parent / "app.py"

    log.info("Starting Streamlit at http://localhost:8501 ...")
    log.info("Press Ctrl+C to stop the dashboard.")

    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(app_path)],
            check=True,
        )
    except KeyboardInterrupt:
        log.info("Dashboard stopped.")
    except subprocess.CalledProcessError:
        log.error("Failed to launch dashboard. Is streamlit installed? Try: uv sync")


if __name__ == "__main__":
    main()

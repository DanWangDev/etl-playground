.PHONY: help install sync test lint fmt clean generate-data run-pipeline benchmark dashboard backfill verify-counts

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install uv if needed and sync dependencies
	@command -v uv >/dev/null 2>&1 || { echo "Installing uv..."; pip install uv; }
	uv sync

sync:  ## Sync dependencies
	uv sync

test:  ## Run all tests with coverage
	uv run pytest --cov=etl_playground --cov-report=term-missing -v

test-quick:  ## Run tests without coverage (faster)
	uv run pytest -v

lint:  ## Lint with ruff
	uv run ruff check src/ tests/

fmt:  ## Format with ruff
	uv run ruff format src/ tests/

clean:  ## Clean generated data and caches
	rm -rf data/raw/* data/curated/* data/rejected/* data/warehouse.duckdb runs/*.json
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

generate-data:  ## Generate synthetic data (default 500K scale)
	uv run python -m etl_playground.m01_data_generator.exercise

generate-data-5m:  ## Generate 5M records for benchmark
	uv run python -m etl_playground.m01_data_generator.exercise --scale 5000000

run-pipeline:  ## Run the full ETL pipeline end-to-end
	uv run python -m etl_playground.m02_spark_foundation.exercise

benchmark:  ## Run CSV vs Parquet benchmark
	uv run python -m etl_playground.m05_benchmark.exercise

dashboard:  ## Launch Streamlit dashboard
	uv run streamlit run src/etl_playground/m09_dashboard/app.py

backfill:  ## Run backfill for a date range
	uv run python -m etl_playground.m06_incremental_etl.exercise --mode backfill

verify-counts:  ## Verify idempotency (run pipeline twice, compare outputs)
	@echo "Running pipeline first time..."
	uv run python -m etl_playground.m06_incremental_etl.exercise --mode full > /tmp/run1.log 2>&1
	@echo "Running pipeline second time..."
	uv run python -m etl_playground.m06_incremental_etl.exercise --mode full > /tmp/run2.log 2>&1
	@echo "Comparing outputs..."
	@diff <(grep "output_rows" /tmp/run1.log) <(grep "output_rows" /tmp/run2.log) && echo "✔ Counts match — idempotent!" || echo "✘ Counts differ — investigate"

.PHONY: help clean install test etl-run etl-backfill

# Default target
help:
	@echo "Available commands:"
	@echo "  make install         - Install dependencies locally"
	@echo "  make test            - Run tests"
	@echo "  make clean           - Remove logs and temporary files"
	@echo ""
	@echo "ETL Commands:"
	@echo "  make etl-run         - Run full ETL pipeline (Iceberg)"
	@echo "  make etl-backfill    - Reprocess Silver and Gold layers"

# Clean up logs and temporary files
clean:
	rm -rf data/logs/*

# Install dependencies locally
install:
	pip install -e .[dev]

# Run tests
test:
	pytest -v

# ============================================================================
# ETL Pipeline Commands
# ============================================================================

# Run full ETL pipeline (Bronze -> Silver -> Gold)
etl-run:
	python run_iceberg_etl.py

# Backfill Silver and Gold layers
etl-backfill:
	python -m app.etl.cli backfill

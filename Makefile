.PHONY: help build up down restart logs clean test db-init

# Default target
help:
	@echo "Available commands:"
	@echo "  make build       - Build Docker images"
	@echo "  make up          - Start all services"
	@echo "  make up-dev      - Start services with pgAdmin"
	@echo "  make up-prefect  - Start services with Prefect server"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - View logs from all services"
	@echo "  make logs-etl    - View ETL application logs"
	@echo "  make logs-db     - View PostgreSQL logs"
	@echo "  make clean       - Remove containers, volumes, and images"
	@echo "  make db-shell    - Connect to PostgreSQL shell"
	@echo "  make etl-shell   - Open shell in ETL container"
	@echo "  make test        - Run tests in container"
	@echo "  make install     - Install dependencies locally"

# Build Docker images
build:
	docker-compose build

# Start services (minimal: just DB and ETL)
up:
	docker-compose up -d postgres etl_app

# Start with development tools (includes pgAdmin)
up-dev:
	docker-compose --profile dev up -d

# Start with Prefect server
up-prefect:
	docker-compose --profile prefect up -d

# Stop all services
down:
	docker-compose down

# Restart services
restart:
	docker-compose restart

# View logs
logs:
	docker-compose logs -f

logs-etl:
	docker-compose logs -f etl_app

logs-db:
	docker-compose logs -f postgres

logs-prefect:
	docker-compose logs -f prefect_server

# Clean up everything
clean:
	docker-compose down -v --rmi local
	rm -rf data/logs/*

# Database shell
db-shell:
	docker-compose exec postgres psql -U postgres -d ride_booking

# ETL application shell
etl-shell:
	docker-compose exec etl_app /bin/bash

# Python shell in ETL container
etl-python:
	docker-compose exec etl_app python

# Run bootstrap in container
bootstrap:
	docker-compose exec etl_app ride-booking-bootstrap

# Run example usage
example:
	docker-compose exec etl_app python app/adapters/example_usage.py

# Run tests
test:
	docker-compose exec etl_app pytest -v

# Install dependencies locally
install:
	pip install -e .[dev]

# Check service status
status:
	docker-compose ps

# View resource usage
stats:
	docker stats --no-stream

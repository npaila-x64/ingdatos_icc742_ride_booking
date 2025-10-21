.PHONY: help build up down restart logs clean test db-init

# Docker Compose command (v2)
DOCKER_COMPOSE := docker compose

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
	$(DOCKER_COMPOSE) build

# Start services (minimal: just DB and ETL)
up:
	$(DOCKER_COMPOSE) up -d postgres etl_app

# Start with development tools (includes pgAdmin)
up-dev:
	$(DOCKER_COMPOSE) --profile dev up -d

# Start with Prefect server
up-prefect:
	$(DOCKER_COMPOSE) --profile prefect up -d

# Stop all services
down:
	$(DOCKER_COMPOSE) down

# Restart services
restart:
	$(DOCKER_COMPOSE) restart

# View logs
logs:
	$(DOCKER_COMPOSE) logs -f

logs-etl:
	$(DOCKER_COMPOSE) logs -f etl_app

logs-db:
	$(DOCKER_COMPOSE) logs -f postgres

logs-prefect:
	$(DOCKER_COMPOSE) logs -f prefect_server

# Clean up everything
clean:
	$(DOCKER_COMPOSE) down -v --rmi local
	rm -rf data/logs/*

# Database shell
db-shell:
	$(DOCKER_COMPOSE) exec postgres psql -U postgres -d ride_booking

# ETL application shell
etl-shell:
	$(DOCKER_COMPOSE) exec etl_app /bin/bash

# Python shell in ETL container
etl-python:
	$(DOCKER_COMPOSE) exec etl_app python

# Run bootstrap in container
bootstrap:
	$(DOCKER_COMPOSE) exec etl_app ride-booking-bootstrap

# Run example usage
example:
	$(DOCKER_COMPOSE) exec etl_app python app/adapters/example_usage.py

# Run tests
test:
	$(DOCKER_COMPOSE) exec etl_app pytest -v

# Install dependencies locally
install:
	pip install -e .[dev]

# Check service status
status:
	$(DOCKER_COMPOSE) ps

# View resource usage
stats:
	docker stats --no-stream

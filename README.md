# Ride Booking ETL with Prefect - Medallion Architecture

A production-ready ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for ride booking analytics. This project processes ride booking data from CSV files into a multi-layered analytical data warehouse using **Prefect** orchestration and **PostgreSQL** storage.

## 🎯 Overview

This repository contains a complete ETL pipeline that:
- **Extracts** ride booking data from CSV files into a Bronze (raw) layer
- **Transforms** data into a normalized Silver (dimensional) layer
- **Aggregates** analytics into a Gold (metrics) layer
- **Orchestrates** the entire pipeline with Prefect workflows

### Key Features

✅ **Medallion Architecture** - Industry-standard data lake pattern (Bronze → Silver → Gold)  
✅ **Prefect Orchestration** - Robust workflow management with retries and monitoring  
✅ **PostgreSQL Storage** - ACID-compliant relational database with schema separation  
✅ **Incremental Processing** - Efficient monthly data partitioning  
✅ **Idempotent Operations** - Safe to re-run with upsert logic  
✅ **Docker Support** - Containerized deployment with Docker Compose  
✅ **Type Safety** - Pydantic models and type hints throughout  

## 📚 Documentation

- **[ETL_README.md](ETL_README.md)** - Complete ETL architecture and usage guide
- **[DOCKER.md](DOCKER.md)** - Docker deployment instructions
- **[examples/](examples/)** - Example scripts and usage patterns

## 🏗️ Architecture

```
CSV Source → Bronze (Raw) → Silver (Normalized) → Gold (Analytics)
              ↓                ↓                    ↓
          Partitioned      Star Schema         Aggregated
          by Month        Fact/Dimension        Metrics
```

**Layers:**
- **Bronze**: Raw data extraction with monthly partitioning
- **Silver**: Normalized dimensional model (customers, bookings, rides, locations, etc.)
- **Gold**: Pre-aggregated analytics (daily summaries, customer metrics, location stats)

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (recommended)
- PostgreSQL 12+ (if running locally)

### Using Docker (Recommended)

```bash
# 1. Clone and navigate to repository
cd ingdatos_icc742_ride_booking

# 2. Copy environment configuration
cp .env.example .env

# 3. Start PostgreSQL database
make up
# or: docker-compose up -d postgres

# 4. Run the ETL pipeline
make etl-run
# or: python -m app.etl.cli run

# 5. (Optional) Run example with diagnostics
make etl-example
# or: python examples/run_etl_example.py
```

**View Results:**
```bash
# Query database
make db-shell
# Then run: SELECT * FROM gold.daily_booking_summary LIMIT 10;

# Or use pgAdmin
make up-dev  # Start pgAdmin at http://localhost:5050
```

### Local Installation

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -e .[dev]

# 3. Configure environment
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 4. Run bootstrap (creates Prefect profile)
ride-booking-bootstrap

# 5. Run ETL pipeline
python -m app.etl.cli run
```

## 📖 Usage

### Command-Line Interface

```bash
# Run full ETL pipeline (Bronze → Silver → Gold)
python -m app.etl.cli run

# Process specific file
python -m app.etl.cli run --source-file data/my_bookings.csv

# Incremental load (for new monthly data)
python -m app.etl.cli incremental --source-file data/2024-10-bookings.csv

# Backfill (reprocess Silver and Gold from existing Bronze)
python -m app.etl.cli backfill

# Skip specific layers
python -m app.etl.cli run --skip-gold
```

### Python API

```python
from app.etl.flows import ride_booking_etl
from datetime import datetime

# Run full pipeline
results = ride_booking_etl(
    source_file="data/ncr_ride_bookings.csv",
    extraction_date=datetime(2024, 10, 21),
    run_bronze=True,
    run_silver=True,
    run_gold=True,
)

print(results)
# Output: Row counts for each layer and table
```

### Makefile Commands

```bash
make help            # Show all available commands
make etl-run         # Run full ETL pipeline
make etl-example     # Run example with diagnostics
make etl-backfill    # Reprocess Silver and Gold layers
make db-query-bronze # Query Bronze layer stats
make db-query-silver # Query Silver layer stats
make db-query-gold   # Query Gold layer stats
```

## 📊 Data Model

### Bronze Layer (Raw Staging)
- `customer` - Unique customers per booking
- `vehicle_type` - Vehicle types used
- `location` - Pickup and drop locations
- `booking` - Main booking records
- `booking_status` - Booking status values
- `payment_method` - Payment methods
- `ride` - Completed ride data
- `cancelled_ride` - Cancellation records
- `incompleted_ride` - Incomplete ride records

All Bronze tables partitioned by `extraction_month` (YYYY-MM format).

### Silver Layer (Normalized Model)

**Dimensions:**
- `customer` - Customer master
- `vehicle_type` - Vehicle type lookup
- `location` - Location lookup
- `booking_status` - Status codes
- `payment_method` - Payment types

**Facts:**
- `booking` - Central fact table with FKs to all dimensions
- `ride` - Completed ride metrics (distance, ratings, TAT)
- `cancelled_ride` - Cancellation details
- `incompleted_ride` - Incomplete ride reasons

### Gold Layer (Analytics)
- `daily_booking_summary` - Daily aggregated metrics
- `customer_analytics` - Customer-level KPIs
- `location_analytics` - Location-level statistics

## PostgreSQL Adapter

The project includes a comprehensive PostgreSQL adapter for database operations:

### Basic Usage
```python
from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings

# Load configuration
settings = load_settings()

# Initialize adapter
adapter = PostgreSQLAdapter(settings.database)

# Read data
df = adapter.read_table("booking", schema="silver")

# Write data
adapter.write_table(df, "processed_bookings", if_exists="append")
```

### Context Manager Pattern
```python
with PostgreSQLAdapter(settings.database) as adapter:
    df = adapter.read_table("rides")
    # Process data...
    adapter.write_table(df, "processed_rides")
```

### Available Methods
- `execute_query()`: Run SELECT queries and get results as dictionaries
- `execute_statement()`: Run INSERT/UPDATE/DELETE statements
- `read_table()`: Load database tables into pandas DataFrames
- `write_table()`: Write pandas DataFrames to database tables
- `table_exists()`: Check if a table exists
- `create_schema()`: Create database schemas

See `app/adapters/example_usage.py` for comprehensive examples.

## Configuration
The project uses environment variables for configuration. Key settings:

### Database Settings
- `DB_HOST`: PostgreSQL host (default: localhost)
- `DB_PORT`: PostgreSQL port (default: 5432)
- `DB_NAME`: Database name (default: ride_booking)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password
- `DB_SCHEMA`: Default schema (default: public)

## Prefect Setup Notes
- The default Prefect profile is `ride-booking-local`; adjust `PREFECT_PROFILE` in
	your `.env` file if you need a different workspace.
- The bootstrap helper creates `data/raw`, `data/processed`, and `data/logs` to
	isolate extracted assets from derived datasets.
- Update `PREFECT_API_URL`, `PREFECT_STORAGE_BLOCK`, and `PREFECT_WORK_POOL` in `.env`
	once infrastructure decisions are finalized.

## Next Steps
- Define Prefect blocks for storage, messaging, and credentials.
- Model ETL flows inside `app/etl` using the project settings helpers.
- Add automated tests around reusable components as flows are introduced.

## Project Structure
```
.
├── app/
│   ├── adapters/          # Database and external service adapters
│   │   ├── postgresql.py  # PostgreSQL adapter
│   │   └── example_usage.py
│   ├── config/            # Configuration management
│   │   └── settings.py
│   └── etl/               # ETL workflows and utilities
│       └── bootstrap.py
├── data/                  # Data files (bronze, silver, gold)
├── init-db/              # Database initialization scripts
├── Dockerfile            # Docker container definition
├── docker-compose.yml    # Multi-container orchestration
├── Makefile              # Convenience commands
├── DOCKER.md             # Docker setup guide
└── README.md             # This file
```

## Documentation
- [Docker Setup Guide](DOCKER.md) - Comprehensive Docker deployment instructions
- [Example Usage](app/adapters/example_usage.py) - PostgreSQL adapter examples
# Ride Booking ETL - Medallion Architecture

A production-ready ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for ride booking analytics, orchestrated with **Prefect** and powered by **PostgreSQL**.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Development](#development)

## 🎯 Overview

This ETL pipeline processes ride booking data from semi-structured CSV files into a multi-layered analytical data warehouse:

- **Bronze Layer**: Raw data extraction with monthly partitioning
- **Silver Layer**: Normalized dimensional model with fact and dimension tables
- **Gold Layer**: Pre-aggregated analytics for fast querying

### Key Features

✅ **Medallion Architecture** - Industry-standard data lake pattern  
✅ **Prefect Orchestration** - Robust workflow management with retries  
✅ **PostgreSQL Storage** - ACID-compliant relational database  
✅ **Incremental Processing** - Efficient monthly data partitioning  
✅ **Idempotent Operations** - Safe to re-run with upsert logic  
✅ **Type Safety** - Pydantic models and type hints throughout  

## 🏗️ Architecture

### Medallion Layers

```
┌─────────────────┐
│  Source Data    │
│  (CSV Files)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      Extract & Partition by Month
│  BRONZE LAYER   │──────────────────────────────────
│  Raw Staging    │      • customer
│                 │      • vehicle_type
│  Partitioned    │      • location
│  by Month       │      • booking
│                 │      • booking_status
│                 │      • payment_method
│                 │      • ride
│                 │      • cancelled_ride
│                 │      • incompleted_ride
└────────┬────────┘
         │
         ▼
┌─────────────────┐      Transform to Dimensional Model
│  SILVER LAYER   │──────────────────────────────────
│  Normalized     │      Dimensions:
│  Model          │      • customer
│                 │      • vehicle_type
│  Star Schema    │      • location
│                 │      • booking_status
│                 │      • payment_method
│                 │
│                 │      Facts:
│                 │      • booking
│                 │      • ride
│                 │      • cancelled_ride
│                 │      • incompleted_ride
└────────┬────────┘
         │
         ▼
┌─────────────────┐      Aggregate Analytics
│   GOLD LAYER    │──────────────────────────────────
│  Analytics      │      • daily_booking_summary
│  Ready          │      • customer_analytics
│                 │      • location_analytics
│  Aggregated     │
│  Metrics        │
└─────────────────┘
```

### Data Flow

1. **Ingestion**: CSV files → Bronze tables (partitioned by `extraction_month`)
2. **Transformation**: Bronze → Silver with deduplication and normalization
3. **Aggregation**: Silver → Gold with pre-computed metrics

## 📊 Data Model

### Bronze Layer

All Bronze tables include:
- `extraction_date`: Date when data was extracted
- `extraction_month`: Partition key (format: `YYYY-MM`)
- `source_file`: Original CSV filename
- `created_at`: Record insertion timestamp

### Silver Layer

#### Dimension Tables
- `customer` - Customer master data
- `vehicle_type` - Vehicle type lookup
- `location` - Location lookup (pickup/drop)
- `booking_status` - Booking status codes
- `payment_method` - Payment method types

#### Fact Tables
- `booking` - Main booking transactions
  - Foreign keys to all dimension tables
  - Stores: date, time, booking_value
- `ride` - Completed ride metrics
  - Links to booking via `booking_id`
  - Metrics: distance, ratings (driver, customer), TAT values
- `cancelled_ride` - Cancellation records
  - Tracks who cancelled and reason
- `incompleted_ride` - Incomplete ride records
  - Tracks incompletion reasons

### Gold Layer

- `daily_booking_summary` - Daily aggregated metrics
- `customer_analytics` - Customer-level KPIs
- `location_analytics` - Location-level statistics

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- PostgreSQL (via Docker)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd ingdatos_icc742_ride_booking

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
```

### 2. Start Database

```bash
# Start PostgreSQL with Bronze/Silver/Gold schemas
docker-compose up -d postgres

# Wait for database to be ready
docker-compose ps
```

The database will automatically:
- Create schemas: `bronze`, `silver`, `gold`
- Run initialization scripts from `init-db/`
- Set up all required tables

### 3. Run ETL Pipeline

```bash
# Run full ETL pipeline
python -m app.etl.cli run

# Or use the flows directly
python -m app.etl.flows
```

## 📖 Usage

### Command-Line Interface

```bash
# Full ETL with all layers
python -m app.etl.cli run

# Process specific file
python -m app.etl.cli run --source-file data/my_bookings.csv

# Incremental load (new data only)
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
# {
#   'bronze': {'bronze.customer': 1500, 'bronze.booking': 1500, ...},
#   'silver': {'silver.customer': 800, 'silver.booking': 1500, ...},
#   'gold': {'gold.daily_booking_summary': 30, ...}
# }
```

### Prefect Deployment

```bash
# Deploy flows to Prefect
python -m app.etl.deploy

# Or use Prefect CLI
prefect deployment build app/etl/flows.py:ride_booking_etl -n "production"
prefect deployment apply ride_booking_etl-deployment.yaml
```

## 📁 Project Structure

```
ingdatos_icc742_ride_booking/
├── app/
│   ├── adapters/
│   │   ├── postgresql.py        # PostgreSQL adapter with SQLAlchemy
│   │   └── example_usage.py     # Adapter usage examples
│   ├── config/
│   │   └── settings.py          # Configuration management
│   ├── etl/
│   │   ├── bootstrap.py         # Prefect profile setup
│   │   ├── bronze_layer.py      # Bronze extraction tasks
│   │   ├── silver_layer.py      # Silver transformation tasks
│   │   ├── gold_layer.py        # Gold aggregation tasks
│   │   ├── flows.py             # Prefect flow orchestration
│   │   ├── deploy.py            # Prefect deployment config
│   │   └── cli.py               # Command-line interface
│   └── __init__.py
├── data/
│   └── ncr_ride_bookings.csv    # Source data
├── init-db/
│   ├── 01_create_schemas.sql    # Schema initialization
│   └── 02_create_medallion_schema.sql  # Table definitions
├── docker-compose.yml           # Docker orchestration
├── Dockerfile                   # Application container
├── pyproject.toml              # Python dependencies
├── prefect.yaml                # Prefect configuration
└── README.md                   # This file
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ride_booking
DB_USER=postgres
DB_PASSWORD=postgres
DB_SCHEMA=public

# Prefect
PREFECT_PROFILE=ride-booking-local
PREFECT_API_URL=http://localhost:4200/api

# Project
PROJECT_BASE_PATH=/path/to/project
PROJECT_DATA_DIR=data
```

### Database Settings

Configure via `app/config/settings.py`:

```python
from app.config.settings import load_settings

settings = load_settings()
print(settings.database.connection_string)
```

## 🛠️ Development

### Run Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# With coverage
pytest --cov=app
```

### Code Quality

```bash
# Format code
ruff format app/

# Lint
ruff check app/

# Type checking
mypy app/
```

### Database Migrations

The project uses SQL scripts in `init-db/` for schema management. To add new tables:

1. Create a new SQL file: `init-db/03_your_changes.sql`
2. Restart the database: `docker-compose restart postgres`
3. Or apply manually: `psql -U postgres -d ride_booking -f init-db/03_your_changes.sql`

### Viewing Data

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d ride_booking

# Query Bronze layer
SELECT COUNT(*) FROM bronze.booking;

# Query Silver layer
SELECT bs.name, COUNT(*) 
FROM silver.booking b 
JOIN silver.booking_status bs ON b.booking_status_id = bs.booking_status_id
GROUP BY bs.name;

# Query Gold layer
SELECT * FROM gold.daily_booking_summary ORDER BY summary_date DESC LIMIT 10;
```

### Using pgAdmin

Access pgAdmin at http://localhost:5050 (if running dev profile):

```bash
docker-compose --profile dev up -d pgadmin
```

Credentials:
- Email: `admin@ridebooking.com`
- Password: `admin`

## 📝 Notes

- **Idempotency**: All ETL operations use upsert logic (INSERT ... ON CONFLICT DO UPDATE)
- **Partitioning**: Bronze data is partitioned by `extraction_month` for efficient querying
- **Incremental Loads**: Process only new data by specifying `extraction_month`
- **Data Quality**: Null values are handled appropriately at each layer
- **Performance**: Batch operations are chunked for large datasets

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

---

**Built with** ❤️ **using Prefect, PostgreSQL, and Python**

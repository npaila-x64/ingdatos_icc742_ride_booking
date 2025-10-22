# Ride Booking ETL with Prefect - Medallion Architecture (Apache Iceberg)

A production-ready ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for ride booking analytics. This project processes ride booking data from CSV files into a multi-layered analytical data lakehouse using **Prefect** orchestration and **Apache Iceberg** storage.

## 🎯 Overview

This repository contains a complete ETL pipeline that:
- **Extracts** ride booking data from CSV files into a Bronze (raw) layer
- **Transforms** data into a normalized Silver (dimensional) layer
- **Aggregates** analytics into a Gold (metrics) layer
- **Orchestrates** the entire pipeline with Prefect workflows

### Key Features

✅ **Medallion Architecture** - Industry-standard data lake pattern (Bronze → Silver → Gold)  
✅ **Apache Iceberg** - Modern table format with ACID transactions and time travel  
✅ **Prefect Orchestration** - Robust workflow management with retries and monitoring  
✅ **Schema Evolution** - Seamless schema changes without rewriting data  
✅ **Time Travel** - Query historical data snapshots  
✅ **Idempotent Operations** - Safe to re-run with upsert logic  
✅ **Type Safety** - Pydantic models and type hints throughout  

## 📚 Documentation

- **[ETL_README.md](ETL_README.md)** - Complete ETL architecture and usage guide
- **[ICEBERG_README.md](ICEBERG_README.md)** - Apache Iceberg implementation details
- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide

## 🏗️ Architecture

```
CSV Source → Bronze (Raw) → Silver (Normalized) → Gold (Analytics)
              ↓                ↓                    ↓
          Iceberg           Iceberg              Iceberg
          Tables            Tables               Tables
```

**Layers:**
- **Bronze**: Raw data extraction stored as Iceberg tables
- **Silver**: Normalized dimensional model (customers, bookings, rides, locations, etc.)
- **Gold**: Pre-aggregated analytics (daily summaries, customer metrics, location stats)

**Storage**: All data stored in Apache Iceberg format in the `warehouse/` directory

## 🚀 Quick Start

### Prerequisites

- Python 3.10+

### Installation

```bash
# 1. Clone and navigate to repository
cd ingdatos_icc742_ride_booking

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -e .[dev]

# 4. Copy environment configuration (optional)
cp .env.example .env

# 5. Run the ETL pipeline
python run_iceberg_etl.py

# Or use make
make etl-run
```

**View Results:**
```python
# Query Iceberg tables
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

settings = load_settings()
adapter = IcebergAdapter(settings.iceberg)

# Read Gold layer
df = adapter.read_table("gold", "daily_booking_summary")
print(df.head())
```

## 📖 Usage

### Command-Line Interface

```bash
# Run full ETL pipeline (Bronze → Silver → Gold)
python run_iceberg_etl.py

# Backfill (reprocess Silver and Gold from existing Bronze)
python -m app.etl.cli backfill
```

### Python API

```python
from app.etl.flows import ride_booking_etl_iceberg
from datetime import datetime

# Run full pipeline
results = ride_booking_etl_iceberg(
    source_file="data/ncr_ride_bookings.csv",
    extraction_date=datetime(2024, 10, 21),
)

print(results)
# Output: Row counts for each layer and table
```

### Makefile Commands

```bash
make help            # Show all available commands
make install         # Install dependencies
make etl-run         # Run full ETL pipeline
make etl-backfill    # Reprocess Silver and Gold layers
make clean           # Clean up logs
```
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

All stored as Apache Iceberg tables in `warehouse/bronze/`.

### Silver Layer (Normalized Model)

**Dimensions:**
- `customer` - Customer master
- `vehicle_type` - Vehicle type lookup
- `location` - Location lookup
- `booking_status` - Status codes
- `payment_method` - Payment types

**Facts:**
- `booking` - Central fact table
- `ride` - Completed ride metrics (distance, ratings, TAT)
- `cancelled_ride` - Cancellation details
- `incompleted_ride` - Incomplete ride reasons

All stored as Apache Iceberg tables in `warehouse/silver/`.

### Gold Layer (Analytics)
- `daily_booking_summary` - Daily aggregated metrics
- `customer_analytics` - Customer-level KPIs
- `location_analytics` - Location-level statistics

All stored as Apache Iceberg tables in `warehouse/gold/`.

## Apache Iceberg Adapter

The project includes a comprehensive Iceberg adapter for data operations:

### Basic Usage
```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

# Load configuration
settings = load_settings()

# Initialize adapter
adapter = IcebergAdapter(settings.iceberg)

# Read data
df = adapter.read_table("silver", "booking")

# Write data (append or overwrite)
adapter.write_table(df, "silver", "booking", mode="append")
```

### Available Methods
- `read_table()`: Load Iceberg tables into pandas DataFrames
- `write_table()`: Write pandas DataFrames to Iceberg tables
- `table_exists()`: Check if a table exists
- `create_table()`: Create new Iceberg tables with schema
- `get_catalog()`: Access PyIceberg catalog for advanced operations

See `app/adapters/iceberg_adapter.py` for comprehensive examples.

## Configuration
The project uses environment variables for configuration. Key settings:

### Iceberg Settings
- `ICEBERG_WAREHOUSE`: Warehouse directory path (default: warehouse)
- `PROJECT_BASE_PATH`: Base project path (default: .)
- `PROJECT_DATA_DIR`: Data directory (default: data)

## Prefect Setup Notes
- The default Prefect profile is `ride-booking-local`; adjust `PREFECT_PROFILE` in
	your `.env` file if you need a different workspace.
- Update `PREFECT_API_URL`, `PREFECT_STORAGE_BLOCK`, and `PREFECT_WORK_POOL` in `.env`
	once infrastructure decisions are finalized.

## Next Steps
- Define Prefect blocks for storage, messaging, and credentials.
- Explore Iceberg time travel and schema evolution features.
- Add automated tests around reusable components as flows are introduced.

## Project Structure
```
.
├── app/
│   ├── adapters/          # Data adapters
│   │   ├── iceberg_adapter.py  # Iceberg adapter
│   │   └── iceberg_schemas.py  # Table schemas
│   ├── config/            # Configuration management
│   │   └── settings.py
│   └── etl/               # ETL workflows and utilities
│       ├── bronze_layer.py
│       ├── silver_layer_iceberg.py
│       ├── gold_layer_iceberg.py
│       └── flows.py
├── data/                  # Data files
├── warehouse/             # Iceberg warehouse
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── run_iceberg_etl.py    # Main ETL runner
├── Makefile              # Convenience commands
├── ICEBERG_README.md     # Iceberg setup guide
└── README.md             # This file
```

## Documentation
- [Iceberg Guide](ICEBERG_README.md) - Apache Iceberg implementation details
- [Quick Start](QUICKSTART.md) - Quick start guide
- [ETL Documentation](ETL_README.md) - Complete ETL guide
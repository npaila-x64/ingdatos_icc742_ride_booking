# Ride Booking ETL with Prefect - Medallion Architecture (Apache Iceberg)

A production-ready ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for ride booking analytics. This project processes ride booking data from CSV files into a multi-layered analytical data lakehouse using **Prefect** orchestration and **Apache Iceberg** storage.

## 🎯 Overview

This repository contains a complete ETL pipeline that:
- **Extracts** ride booking data from CSV files into a Bronze (raw) layer
- **Transforms** data into a normalized Silver (dimensional) layer
- **Aggregates** analytics into a Gold (metrics) layer
- **Orchestrates** the entire pipeline with Prefect workflows

### Key Features

- **Medallion Architecture** Industry-standard data lake pattern (Bronze → Silver → Gold)  
- **Apache Iceberg** Modern table format with ACID transactions and time travel  
- **Prefect Orchestration** Robust workflow management with retries and monitoring  
- **Schema Evolution** Seamless schema changes without rewriting data  
- **Time Travel** Query historical data snapshots  
- **Idempotent Operations** Safe to re-run with upsert logic  
- **Type Safety** Pydantic models and type hints throughout  

## 📚 Documentation

- **[ETL_ARCHITECTURE.md](ETL_ARCHITECTURE.md)** - Architecture details
- **[streamlit_apps/](streamlit_apps/)** - Web visualization dashboards

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- pip

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd ingdatos_icc742_ride_booking
```

2. **Create and activate virtual environment**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/Mac
# OR
venv\Scripts\activate     # On Windows
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
# OR if using pyproject.toml
pip install -e .
```

### Running the ETL Pipeline

**Important:** Always activate the virtual environment before running any commands:
```bash
source venv/bin/activate
```

#### Run Complete ETL Pipeline (All Data)

Process all data in the CSV file regardless of date:
```bash
python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --no-date-filter
```

#### Run Complete ETL Pipeline (Specific Month)

Process only data from a specific month:
```bash
# Process March 2024 data
python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --extraction-date 2024-03-01

# Process any other month
python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --extraction-date 2024-06-15
```

#### Run Individual Layers

```bash
# Bronze layer only (extract raw data)
python3 -m app.etl.cli bronze --source-file data/ncr_ride_bookings.csv --no-date-filter

# Silver layer only (transform to dimensional model)
python3 -m app.etl.cli silver

# Gold layer only (aggregate analytics)
python3 -m app.etl.cli gold
```

#### Advanced Options

```bash
# Run incremental ETL (new data only)
python3 -m app.etl.cli incremental --source-file data/new_bookings.csv --extraction-date 2024-03-01

# Run backfill (reprocess Silver + Gold from existing Bronze)
python3 -m app.etl.cli backfill

# Specify custom warehouse location
python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --warehouse /path/to/warehouse --no-date-filter
```

### CLI Command Reference

```bash
python3 -m app.etl.cli <command> [options]

Commands:
  run         - Run complete ETL pipeline (Bronze → Silver → Gold)
  bronze      - Run Bronze layer extraction only
  silver      - Run Silver layer transformation only
  gold        - Run Gold layer aggregation only
  incremental - Run incremental ETL (append new data)
  backfill    - Reprocess Silver + Gold from existing Bronze

Options:
  --source-file, -s      Path to source CSV file
  --extraction-date, -d  Extraction date (YYYY-MM-DD format)
  --extraction-month, -m Extraction month (YYYY-MM format)
  --warehouse, -w        Path to Iceberg warehouse directory
  --no-date-filter       Process all data regardless of date (recommended for initial load)

Examples:
  # Initial load with all data
  python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --no-date-filter
  
  # Monthly incremental load
  python3 -m app.etl.cli run --source-file data/ncr_ride_bookings.csv --extraction-date 2024-03-01
  
  # Reprocess transformations
  python3 -m app.etl.cli backfill
```

### Expected Output

After successful execution, you should see a summary like:

```
================================================================================
ETL EXECUTION SUMMARY
================================================================================

BRONZE Layer:
  bronze.customer: 150,000 rows
  bronze.vehicle_type: 149,791 rows
  bronze.location: 299,983 rows
  bronze.booking_status: 149,498 rows
  bronze.payment_method: 101,865 rows
  bronze.booking: 148,767 rows
  bronze.ride: 92,551 rows
  bronze.cancelled_ride: 37,427 rows
  bronze.incompleted_ride: 8,994 rows

SILVER Layer:
  silver.customer: 147,580 rows
  silver.vehicle_type: 7 rows
  silver.location: 176 rows
  silver.booking_status: 5 rows
  silver.payment_method: 5 rows
  silver.booking: 148,767 rows
  silver.ride: 92,551 rows
  silver.cancelled_ride: 37,427 rows
  silver.incompleted_ride: 8,994 rows

GOLD Layer:
  gold.daily_booking_summary: 12,016 rows
  gold.customer_analytics: 147,580 rows
  gold.location_analytics: 176 rows
================================================================================
```

## 🎨 Visualization & Querying

Interactive web-based tools for exploring your Iceberg data.

**Note:** Make sure your virtual environment is activated before running these tools:
```bash
source venv/bin/activate
```

### 📊 Analytics Dashboard
Pre-built visualizations for business insights:
```bash
./run_dashboard.sh
# Open http://localhost:8501
```

### 🔍 SQL Query Interface
Custom SQL queries with DuckDB:
```bash
./run_sql_query.sh
# Open http://localhost:8502
```

See **[streamlit_apps/README.md](streamlit_apps/README.md)** for detailed usage instructions.

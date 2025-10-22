# Quick Start Guide - Ride Booking ETL (Apache Iceberg)

Get up and running with the Ride Booking ETL pipeline using Apache Iceberg in 5 minutes.

## 1. Prerequisites Check

```bash
# Check Python version (requires 3.10+)
python --version
```

## 2. Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Set up environment variables (optional)
cp .env.example .env
# Edit .env file if needed
```

## 3. Run Your First ETL

```bash
# Using Python directly
python run_iceberg_etl.py

# Or using make
make etl-run
```

Expected output:
```
================================================================================
BRONZE LAYER: Extracting raw data to Iceberg
================================================================================
Loaded 150,001 rows from source file
Bronze extraction completed: 150,000+ total rows

================================================================================
SILVER LAYER: Transforming to dimensional model in Iceberg
================================================================================
Transformed X customers to Silver
Transformed X bookings to Silver
...

================================================================================
GOLD LAYER: Aggregating analytics in Iceberg
================================================================================
Aggregated X daily summaries to Gold
...

================================================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
================================================================================
```

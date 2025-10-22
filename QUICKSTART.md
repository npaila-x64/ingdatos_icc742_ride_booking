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

## 4. Verify Results

### Query Iceberg Tables with Python

```python
from pyiceberg.catalog import load_catalog

# Load catalog
catalog = load_catalog("default", 
    type="rest",
    uri="http://localhost:8181",
    warehouse="warehouse"
)

# Query Gold layer
table = catalog.load_table("gold.daily_booking_summary")
df = table.scan().to_pandas()
print(df.head())
```

### Explore with PyArrow

```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

settings = load_settings()
adapter = IcebergAdapter(settings.iceberg)

# Read Gold layer table
df = adapter.read_table("gold", "daily_booking_summary")
print(f"Total rows: {len(df)}")
print(df.head())
```

## 5. Explore the Data

### Check Layer Statistics

```python
import pyarrow.parquet as pq
from pathlib import Path

# Bronze layer stats
bronze_path = Path("warehouse/bronze/booking/data")
if bronze_path.exists():
    for file in bronze_path.glob("*.parquet"):
        table = pq.read_table(file)
        print(f"Bronze booking: {len(table)} rows")

# Similar for silver and gold
```

### Sample Analysis Queries

```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

settings = load_settings()
adapter = IcebergAdapter(settings.iceberg)

# Top customers by total bookings
customer_analytics = adapter.read_table("gold", "customer_analytics")
top_customers = customer_analytics.nlargest(10, 'total_bookings')
print(top_customers[['customer_id', 'total_bookings', 'total_spent']])

# Daily revenue trend
daily_summary = adapter.read_table("gold", "daily_booking_summary")
print(daily_summary.sort_values('summary_date', ascending=False).head(10))

# Busiest locations
location_analytics = adapter.read_table("gold", "location_analytics")
top_locations = location_analytics.nlargest(10, 'total_pickups')
print(top_locations[['location_name', 'total_pickups', 'total_drops']])
```

## 6. Next Steps

### Run with Different Options

```bash
# Run with backfill
python -m app.etl.cli backfill
```

### Explore Iceberg Features

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default", warehouse="warehouse")

# List all tables
tables = catalog.list_tables()
print(f"Total tables: {len(tables)}")

# Inspect table schema
table = catalog.load_table("silver.booking")
print(table.schema())

# Check table history (time travel)
print(table.history())

# Read specific snapshot
snapshot_id = table.current_snapshot().snapshot_id
df = table.scan(snapshot_id=snapshot_id).to_pandas()
```

## Common Issues & Solutions

### Issue: PyIceberg not installed

**Solution:**
```bash
# Install dependencies
pip install -e .

# Or specifically
pip install pyiceberg pyarrow pandas
```

### Issue: Source file not found

**Solution:**
```bash
# Check file exists
ls -lh data/ncr_ride_bookings.csv

# Use absolute path if needed
python run_iceberg_etl.py
```

### Issue: Import errors

**Solution:**
```bash
# Reinstall dependencies
pip install -e .

# Verify installation
python -c "import pyiceberg; print(pyiceberg.__version__)"
```

## Help & Documentation

- **Full ETL Documentation**: [ETL_README.md](ETL_README.md)
- **Iceberg Guide**: [ICEBERG_README.md](ICEBERG_README.md)
- **Main README**: [README.md](README.md)
- **Run Script**: [run_iceberg_etl.py](run_iceberg_etl.py)

### Get Help

```bash
# CLI help
python -m app.etl.cli --help

# Makefile commands
make help
```

## Summary

You've now:
1. ✅ Set up the Python environment
2. ✅ Run the ETL pipeline with Iceberg
3. ✅ Verified data in Bronze/Silver/Gold layers
4. ✅ Queried analytics results

**Your Iceberg data lakehouse is ready!** 🎉

Next, explore the [ICEBERG_README.md](ICEBERG_README.md) for advanced features like:
- Time travel and versioning
- Schema evolution
- Partition pruning
- Prefect orchestration and scheduling


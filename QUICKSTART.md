## 📊 Architecture Overview

### Task Breakdown

```
Bronze Layer (10 tasks)
├── load_and_prepare_source_data    ← Preprocess CSV
├── extract_bronze_customer          ← Customer data
├── extract_bronze_vehicle_type      ← Vehicle types
├── extract_bronze_location          ← Locations
├── extract_bronze_booking_status    ← Booking statuses
├── extract_bronze_payment_method    ← Payment methods
├── extract_bronze_booking           ← Booking facts
├── extract_bronze_ride              ← Ride facts
├── extract_bronze_cancelled_ride    ← Cancellations
└── extract_bronze_incompleted_ride  ← Incomplete rides

Silver Layer (9 tasks - 2 phases)
Phase 1: Dimensions (parallel)
├── transform_silver_customer
├── transform_silver_vehicle_type
├── transform_silver_location
├── transform_silver_booking_status
└── transform_silver_payment_method

Phase 2: Facts (parallel, after dimensions)
├── transform_silver_booking
├── transform_silver_ride
├── transform_silver_cancelled_ride
└── transform_silver_incompleted_ride

Gold Layer (3 tasks - parallel)
├── aggregate_gold_daily_booking_summary
├── aggregate_gold_customer_analytics
└── aggregate_gold_location_analytics
```

## 🖥️ CLI Usage

### CLI

```bash
# Run complete pipeline (all 22 tasks)
python -m app.etl.cli_granular run --source-file data/ncr_ride_bookings.csv

# Run only Bronze layer (10 tasks)
python -m app.etl.cli_granular bronze --source-file data/ncr_ride_bookings.csv

# Run only Silver layer (9 tasks)
python -m app.etl.cli_granular silver

# Run only Silver for specific month
python -m app.etl.cli_granular silver --extraction-month 2024-11

# Run only Gold layer (3 tasks)
python -m app.etl.cli_granular gold

# Incremental ETL (new data)
python -m app.etl.cli_granular incremental \
    --source-file data/new_bookings.csv \
    --extraction-date 2024-12-01

# Backfill (Silver + Gold only)
python -m app.etl.cli_granular backfill
```

## 🐍 Python API

### Complete Pipeline

```python
from app.etl.flows_granular import granular_ride_booking_etl
from datetime import datetime

results = granular_ride_booking_etl(
    source_file="data/ncr_ride_bookings.csv",
    extraction_date=datetime(2024, 12, 1),
    run_bronze=True,
    run_silver=True,
    run_gold=True,
)

print(f"Bronze: {sum(results['bronze'].values()):,} rows")
print(f"Silver: {sum(results['silver'].values()):,} rows")
print(f"Gold: {sum(results['gold'].values()):,} rows")
```

### Individual Layers

```python
from app.etl.flows_granular import (
    bronze_extraction_flow,
    silver_transformation_flow,
    gold_aggregation_flow,
)
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings
from datetime import datetime
from pathlib import Path

settings = load_settings()
iceberg = IcebergAdapter(settings.iceberg)

# Bronze only (10 tasks in parallel)
bronze_results = bronze_extraction_flow(
    source_file=Path("data/ncr_ride_bookings.csv"),
    iceberg_adapter=iceberg,
    extraction_date=datetime(2024, 12, 1),
)

# Silver only (9 tasks in 2 phases)
silver_results = silver_transformation_flow(
    iceberg_adapter=iceberg,
    extraction_month="2024-12",
)

# Gold only (3 tasks in parallel)
gold_results = gold_aggregation_flow(
    iceberg_adapter=iceberg,
    target_date=datetime(2024, 12, 1),
)
```

### Individual Tasks

```python
from app.etl.tasks.bronze import (
    load_and_prepare_source_data,
    extract_bronze_customer,
)
from app.etl.tasks.silver.dimensions import transform_silver_customer
from app.etl.tasks.gold import aggregate_gold_customer_analytics
from app.adapters.iceberg_adapter import IcebergAdapter
from pathlib import Path
from datetime import datetime

iceberg = IcebergAdapter(warehouse_path="./warehouse")

# Preprocess data once
prepared_df = load_and_prepare_source_data(
    source_file=Path("data/ncr_ride_bookings.csv"),
    extraction_date=datetime(2024, 12, 1),
)

# Extract specific entity
customer_count = extract_bronze_customer(prepared_df, iceberg)

# Transform specific dimension
customer_dim_count = transform_silver_customer(iceberg, "2024-12")

# Aggregate specific analytics
customer_analytics = aggregate_gold_customer_analytics(iceberg)
```

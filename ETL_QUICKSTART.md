# Granular ETL - Quick Start Guide

## 🚀 What's New?

The ETL pipeline has been enhanced with **granular, entity-level tasks** that provide:

- ✅ **22 individual tasks** instead of 3 monolithic functions
- ✅ **Parallel execution** within layers for faster processing
- ✅ **Fine-grained monitoring** - track each entity separately
- ✅ **Entity-level retries** - retry only failed entities
- ✅ **Flexible control** - run individual layers or entities
- ✅ **Backward compatible** - old code still works

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

### New Granular CLI (Recommended)

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

### Legacy CLI (Still Works)

```bash
# Old CLI still works - uses granular implementation internally
python -m app.etl.cli run --source-file data/ncr_ride_bookings.csv
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

## 🧪 Testing

### Run Verification Tests

```bash
# Verify all imports and configurations
python test_granular_etl.py

# Test full execution (runs complete pipeline)
python test_granular_flow.py
```

Expected output:
```
================================================================================
✅ ALL VERIFICATION TESTS PASSED
================================================================================
```

## 📈 Performance Benefits

### Sequential vs Parallel Execution

**Before (Sequential):**
- Bronze: 9 entities × ~1s = ~9 seconds
- Silver: 9 tables × ~1s = ~9 seconds
- Gold: 3 tables × ~1s = ~3 seconds
- **Total: ~21 seconds**

**After (Parallel):**
- Bronze: max(9 entities) = ~1-2 seconds
- Silver: max(5 dims, 4 facts) = ~2-3 seconds
- Gold: max(3 tables) = ~1 second
- **Total: ~4-6 seconds (60-70% faster!)**

## 🎯 Key Features

### 1. Parallel Execution
Tasks within each layer run concurrently for faster processing.

### 2. Dependency Management
Prefect automatically manages dependencies:
- Silver facts wait for Silver dimensions
- No manual coordination needed

### 3. Task-Level Retries
Each task has individual retry logic (retries=2, retry_delay=30s).

### 4. Fine-Grained Monitoring
Track individual entity processing in Prefect UI or logs.

### 5. Backward Compatibility
Old code continues to work without changes.

## 📚 Documentation

- **[GRANULAR_ETL_ARCHITECTURE.md](./GRANULAR_ETL_ARCHITECTURE.md)** - Complete architecture guide
- **[GRANULAR_ETL_SUMMARY.md](./GRANULAR_ETL_SUMMARY.md)** - Summary of changes
- **[ETL_ARCHITECTURE.md](./ETL_ARCHITECTURE.md)** - Updated main documentation

## 🔍 Troubleshooting

### Verify Installation

```bash
python test_granular_etl.py
```

### Check Task Imports

```python
from app.etl.tasks.bronze import extract_bronze_customer
from app.etl.tasks.silver import transform_silver_customer
from app.etl.tasks.gold import aggregate_gold_customer_analytics
```

### Test CLI

```bash
python -m app.etl.cli_granular --help
```

## 💡 Examples

### Example 1: Run Only Customer Pipeline

```python
from app.etl.tasks.bronze import load_and_prepare_source_data, extract_bronze_customer
from app.etl.tasks.silver.dimensions import transform_silver_customer
from app.etl.tasks.gold import aggregate_gold_customer_analytics

# Preprocess
df = load_and_prepare_source_data(...)

# Extract
extract_bronze_customer(df, iceberg)

# Transform
transform_silver_customer(iceberg)

# Aggregate
aggregate_gold_customer_analytics(iceberg)
```

### Example 2: Monthly Incremental Load

```bash
# November data
python -m app.etl.cli_granular run \
    --source-file data/november_2024.csv \
    --extraction-date 2024-11-01

# December data
python -m app.etl.cli_granular run \
    --source-file data/december_2024.csv \
    --extraction-date 2024-12-01
```

### Example 3: Reprocess Only Silver and Gold

```bash
# Skip Bronze, reprocess Silver and Gold
python -m app.etl.cli_granular backfill
```

## 🎉 Success!

You're now ready to use the granular ETL pipeline. Start with:

```bash
python -m app.etl.cli_granular run --source-file data/ncr_ride_bookings.csv
```

Watch as 22 individual tasks execute efficiently in parallel! 🚀

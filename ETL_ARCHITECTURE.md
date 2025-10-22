# Granular ETL Architecture - Entity-Level Task Design

## 📋 Overview

This document describes the **granular, entity-level ETL architecture** that breaks down the monolithic Bronze → Silver → Gold pipeline into individual, reusable tasks for each entity and table. This design enables:

- **Fine-grained monitoring** - Track individual entity processing
- **Parallel execution** - Run independent tasks concurrently
- **Easier debugging** - Isolate failures to specific entities
- **Entity-level retries** - Retry only failed entities
- **Flexible orchestration** - Run full pipeline or individual layers/entities

---

## 🏗️ Task Organization

### Directory Structure

```
app/etl/tasks/
├── __init__.py              # Top-level task exports
├── bronze/                  # Bronze layer tasks
│   ├── __init__.py
│   ├── preprocessing.py     # Shared: Load and prepare source data
│   ├── customer.py          # Extract customer data
│   ├── vehicle_type.py      # Extract vehicle types
│   ├── location.py          # Extract locations (pickup/drop)
│   ├── booking_status.py    # Extract booking statuses
│   ├── payment_method.py    # Extract payment methods
│   ├── booking.py           # Extract booking facts
│   ├── ride.py              # Extract ride facts
│   ├── cancelled_ride.py    # Extract cancelled rides
│   └── incompleted_ride.py  # Extract incompleted rides
├── silver/                  # Silver layer tasks
│   ├── __init__.py
│   ├── dimensions.py        # All dimension transformations
│   │   ├── transform_silver_customer
│   │   ├── transform_silver_vehicle_type
│   │   ├── transform_silver_location
│   │   ├── transform_silver_booking_status
│   │   └── transform_silver_payment_method
│   └── facts.py             # All fact transformations
│       ├── transform_silver_booking
│       ├── transform_silver_ride
│       ├── transform_silver_cancelled_ride
│       └── transform_silver_incompleted_ride
└── gold/                    # Gold layer tasks
    ├── __init__.py
    ├── daily_booking_summary.py   # Daily aggregations
    ├── customer_analytics.py      # Customer analytics
    └── location_analytics.py      # Location analytics
```

---

## 🥉 Bronze Layer - Granular Extraction Tasks

### Task Flow

```
┌────────────────────────────────────────────────────────────────┐
│ 1. load_and_prepare_source_data (preprocessing.py)            │
│    • Load CSV file                                             │
│    • Clean columns and data                                    │
│    • Add extraction metadata (date, month, source_file)        │
│    • Return prepared DataFrame                                 │
└────────────────────┬───────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────┐
│ 2. Parallel Entity Extractions (all receive prepared_df)      │
│                                                                 │
│ ┌──────────────────────────────────────────────────────────┐  │
│ │ Dimension Extractions (parallel)                         │  │
│ ├──────────────────────────────────────────────────────────┤  │
│ │ • extract_bronze_customer                                │  │
│ │ • extract_bronze_vehicle_type                            │  │
│ │ • extract_bronze_location (pickup + drop)                │  │
│ │ • extract_bronze_booking_status                          │  │
│ │ • extract_bronze_payment_method                          │  │
│ └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│ ┌──────────────────────────────────────────────────────────┐  │
│ │ Fact Extractions (parallel)                              │  │
│ ├──────────────────────────────────────────────────────────┤  │
│ │ • extract_bronze_booking                                 │  │
│ │ • extract_bronze_ride (completed rides only)             │  │
│ │ • extract_bronze_cancelled_ride                          │  │
│ │ • extract_bronze_incompleted_ride                        │  │
│ └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### Individual Bronze Tasks

| Task | Entity | Table | Description |
|------|--------|-------|-------------|
| `extract_bronze_customer` | Customer | `bronze.customer` | Customer IDs per booking |
| `extract_bronze_vehicle_type` | Vehicle Type | `bronze.vehicle_type` | Vehicle types per booking |
| `extract_bronze_location` | Location | `bronze.location` | Pickup and drop locations |
| `extract_bronze_booking_status` | Booking Status | `bronze.booking_status` | Status values per booking |
| `extract_bronze_payment_method` | Payment Method | `bronze.payment_method` | Payment methods per booking |
| `extract_bronze_booking` | Booking | `bronze.booking` | Main booking fact table |
| `extract_bronze_ride` | Ride | `bronze.ride` | Completed rides with metrics |
| `extract_bronze_cancelled_ride` | Cancelled Ride | `bronze.cancelled_ride` | Cancelled booking details |
| `extract_bronze_incompleted_ride` | Incompleted Ride | `bronze.incompleted_ride` | Incomplete ride reasons |

**Key Features:**
- All tasks are **Prefect @task** decorated with `retries=2`
- All tasks receive the **same prepared DataFrame**
- All tasks write to Iceberg in **append mode**
- All tasks are **idempotent** and **partition-aware**

---

## 🥈 Silver Layer - Granular Transformation Tasks

### Task Flow with Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Dimension Transformations (parallel)               │
├─────────────────────────────────────────────────────────────┤
│ • transform_silver_customer                                 │
│ • transform_silver_vehicle_type                             │
│ • transform_silver_location                                 │
│ • transform_silver_booking_status                           │
│ • transform_silver_payment_method                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ All dimensions complete
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Fact Transformations (parallel, after dims)       │
├─────────────────────────────────────────────────────────────┤
│ • transform_silver_booking (needs dim lookups)              │
│ • transform_silver_ride                                     │
│ • transform_silver_cancelled_ride                           │
│ • transform_silver_incompleted_ride                         │
└─────────────────────────────────────────────────────────────┘
```

### Individual Silver Tasks

#### Dimension Tasks

| Task | Entity | Table | Key Operation |
|------|--------|-------|---------------|
| `transform_silver_customer` | Customer | `silver.customer` | Aggregate booking metrics (first/last seen, total bookings) |
| `transform_silver_vehicle_type` | Vehicle Type | `silver.vehicle_type` | Deduplicate + assign surrogate keys (1..N) |
| `transform_silver_location` | Location | `silver.location` | Deduplicate + assign surrogate keys (1..N) |
| `transform_silver_booking_status` | Booking Status | `silver.booking_status` | Deduplicate + assign surrogate keys (1..N) |
| `transform_silver_payment_method` | Payment Method | `silver.payment_method` | Deduplicate + assign surrogate keys (1..N) |

#### Fact Tasks

| Task | Entity | Table | Key Operation |
|------|--------|-------|---------------|
| `transform_silver_booking` | Booking | `silver.booking` | Map Bronze natural keys to Silver surrogate keys (FK lookups) |
| `transform_silver_ride` | Ride | `silver.ride` | Copy ride metrics with timestamps |
| `transform_silver_cancelled_ride` | Cancelled Ride | `silver.cancelled_ride` | Generate `cancellation_id`, copy cancellation data |
| `transform_silver_incompleted_ride` | Incompleted Ride | `silver.incompleted_ride` | Generate `incompleted_id`, copy incompletion data |

**Key Features:**
- **Dependency ordering:** Facts wait for dimensions to complete
- All tasks write in **overwrite mode** (clean data)
- All tasks add **created_at** and **updated_at** timestamps
- Prefect automatically handles task dependencies

---

## 🥇 Gold Layer - Granular Aggregation Tasks

### Task Flow

```
┌─────────────────────────────────────────────────────────────┐
│ All Analytics Aggregations (parallel)                       │
├─────────────────────────────────────────────────────────────┤
│ • aggregate_gold_daily_booking_summary                      │
│ • aggregate_gold_customer_analytics                         │
│ • aggregate_gold_location_analytics                         │
└─────────────────────────────────────────────────────────────┘
```

### Individual Gold Tasks

| Task | Entity | Table | Grain | Metrics |
|------|--------|-------|-------|---------|
| `aggregate_gold_daily_booking_summary` | Daily Summary | `gold.daily_booking_summary` | date × vehicle_type × status | total_bookings, total_revenue, avg_booking_value |
| `aggregate_gold_customer_analytics` | Customer Analytics | `gold.customer_analytics` | customer_id | total_bookings, total_spent, avg_booking_value, lifetime_days |
| `aggregate_gold_location_analytics` | Location Analytics | `gold.location_analytics` | location_id | pickups, dropoffs, total_activity, avg_booking_value |

**Key Features:**
- All aggregations can run **in parallel**
- All tasks read from **Silver layer tables**
- All tasks write in **overwrite mode** (refresh analytics)
- Pre-computed aggregations for fast querying

---

## 🔄 Orchestration Flows

### Main Flow

```python
from app.etl.flows import granular_ride_booking_etl

# Run complete pipeline
results = granular_ride_booking_etl(
    source_file="data/ncr_ride_bookings.csv",
    extraction_date=datetime(2024, 12, 1),
    run_bronze=True,
    run_silver=True,
    run_gold=True,
)
```

### Layer-Specific Flows

```python
from app.etl.flows import (
    bronze_extraction_flow,
    silver_transformation_flow,
    gold_aggregation_flow,
)
from app.adapters.iceberg_adapter import IcebergAdapter

iceberg = IcebergAdapter(warehouse_path="./warehouse")

# Run only Bronze
bronze_results = bronze_extraction_flow(
    source_file=Path("data/ncr_ride_bookings.csv"),
    iceberg_adapter=iceberg,
    extraction_date=datetime(2024, 12, 1),
)

# Run only Silver
silver_results = silver_transformation_flow(
    iceberg_adapter=iceberg,
    extraction_month="2024-12",  # Optional filter
)

# Run only Gold
gold_results = gold_aggregation_flow(
    iceberg_adapter=iceberg,
    target_date=datetime(2024, 12, 1),  # Optional filter
)
```

---

## 🖥️ CLI Usage

### Enhanced Granular CLI

```bash
# Run complete granular ETL
python -m app.etl.cli_granular run --source-file data/ncr_ride_bookings.csv

# Run only Bronze layer
python -m app.etl.cli bronze --source-file data/ncr_ride_bookings.csv \
    --extraction-date 2024-12-01

# Run only Silver layer (all months)
python -m app.etl.cli silver

# Run only Silver layer (specific month)
python -m app.etl.cli silver --extraction-month 2024-11

# Run only Gold layer
python -m app.etl.cli gold

# Run incremental ETL (Bronze → Silver → Gold)
python -m app.etl.cli incremental \
    --source-file data/december_bookings.csv \
    --extraction-date 2024-12-01

# Run backfill (Silver + Gold only, skip Bronze)
python -m app.etl.cli backfill

# Specify custom warehouse path
python -m app.etl.cli run \
    --source-file data/ncr_ride_bookings.csv \
    --warehouse ./custom_warehouse
```

---

## 🎯 Benefits of Granular Architecture

### 1. **Observability**

- Each entity has its own task with individual logs
- Easy to identify which entity failed
- Prefect UI shows task-level execution graph

### 2. **Parallel Execution**

- Bronze: 9 tasks can run concurrently (limited by I/O)
- Silver: 5 dimensions + 4 facts (facts wait for dims)
- Gold: 3 aggregations run concurrently

### 3. **Selective Reprocessing**

```python
# Reprocess only specific entities
from app.etl.tasks.bronze import extract_bronze_customer
from app.etl.tasks.silver.dimensions import transform_silver_customer

# Re-extract customer data
extract_bronze_customer(prepared_df, iceberg)

# Re-transform customer dimension
transform_silver_customer(iceberg, extraction_month="2024-12")
```

### 4. **Entity-Level Retries**

- Each task has `retries=2, retry_delay_seconds=30`
- If `extract_bronze_ride` fails, only that entity retries
- Other entities continue processing

### 5. **Incremental Development**

- Add new entities by creating new task files
- No need to modify monolithic layer functions
- Easy to test individual entities

---

## 📊 Execution Example

### Complete Pipeline

```
2024-12-01 10:00:00 - Starting GRANULAR ETL pipeline
2024-12-01 10:00:01 - BRONZE LAYER: Extracting raw data
2024-12-01 10:00:02 - [load_and_prepare_source_data] Loaded 150,000 rows
2024-12-01 10:00:03 - [extract_bronze_customer] Wrote 150,000 rows
2024-12-01 10:00:03 - [extract_bronze_vehicle_type] Wrote 149,973 rows
2024-12-01 10:00:04 - [extract_bronze_location] Wrote 300,000 rows
2024-12-01 10:00:04 - [extract_bronze_booking_status] Wrote 149,961 rows
2024-12-01 10:00:05 - [extract_bronze_payment_method] Wrote 101,990 rows
2024-12-01 10:00:06 - [extract_bronze_booking] Wrote 149,885 rows
2024-12-01 10:00:07 - [extract_bronze_ride] Wrote 92,969 rows
2024-12-01 10:00:08 - [extract_bronze_cancelled_ride] Wrote 37,492 rows
2024-12-01 10:00:08 - [extract_bronze_incompleted_ride] Wrote 8,999 rows
2024-12-01 10:00:09 - Bronze completed: 1,141,269 total rows

2024-12-01 10:00:10 - SILVER LAYER: Transforming to dimensional model
2024-12-01 10:00:11 - [transform_silver_customer] Wrote 148,678 rows
2024-12-01 10:00:11 - [transform_silver_vehicle_type] Wrote 7 rows
2024-12-01 10:00:12 - [transform_silver_location] Wrote 176 rows
2024-12-01 10:00:12 - [transform_silver_booking_status] Wrote 5 rows
2024-12-01 10:00:12 - [transform_silver_payment_method] Wrote 5 rows
2024-12-01 10:00:15 - [transform_silver_booking] Wrote 149,885 rows
2024-12-01 10:00:16 - [transform_silver_ride] Wrote 92,969 rows
2024-12-01 10:00:16 - [transform_silver_cancelled_ride] Wrote 37,492 rows
2024-12-01 10:00:17 - [transform_silver_incompleted_ride] Wrote 8,999 rows
2024-12-01 10:00:17 - Silver completed: 438,216 total rows

2024-12-01 10:00:18 - GOLD LAYER: Aggregating analytics
2024-12-01 10:00:20 - [aggregate_gold_daily_booking_summary] Wrote 12,022 rows
2024-12-01 10:00:22 - [aggregate_gold_customer_analytics] Wrote 148,678 rows
2024-12-01 10:00:23 - [aggregate_gold_location_analytics] Wrote 176 rows
2024-12-01 10:00:23 - Gold completed: 160,876 total rows

2024-12-01 10:00:24 - GRANULAR ETL PIPELINE COMPLETED SUCCESSFULLY
```

---

## 🔀 Migration from Monolithic

### Before (Monolithic)

```python
# Old monolithic functions
from app.etl.bronze_layer import extract_to_bronze
from app.etl.silver_layer_iceberg import transform_to_silver
from app.etl.gold_layer_iceberg import aggregate_to_gold

# All-or-nothing execution
bronze_counts = extract_to_bronze(...)      # 9 entities in one task
silver_counts = transform_to_silver(...)    # 9 tables in one task
gold_counts = aggregate_to_gold(...)        # 3 tables in one task
```

### After (Granular)

```python
# New granular flows
from app.etl.flows_granular import (
    granular_ride_booking_etl,
    bronze_extraction_flow,       # Orchestrates 10 tasks (1 prep + 9 extractions)
    silver_transformation_flow,    # Orchestrates 9 tasks (5 dims + 4 facts)
    gold_aggregation_flow,         # Orchestrates 3 tasks
)

# Flexible execution with 22 individual tasks
results = granular_ride_booking_etl(...)
```

**Backward Compatibility:** The old `ride_booking_etl()` flow now delegates to `granular_ride_booking_etl()`.

---

## 📈 Performance Considerations

### Parallel Execution Speedup

Assuming 9 Bronze extractions take 1 second each:

- **Sequential (old):** 9 seconds total
- **Parallel (new):** ~1-2 seconds total (limited by CPU/IO)

Actual speedup depends on:
- Number of CPU cores
- I/O throughput (disk, network)
- Prefect task runner configuration (LocalDaskTaskRunner, RayTaskRunner)

### Resource Usage

- **Memory:** Each task processes a subset of data (lower per-task memory)
- **CPU:** Better utilization through parallel execution
- **I/O:** Iceberg handles concurrent writes efficiently

---

## 🚀 Future Enhancements

### 1. Entity-Specific Schedules

```python
# Run customer analytics hourly
@flow(schedule="0 * * * *")
def hourly_customer_analytics():
    aggregate_gold_customer_analytics(iceberg)

# Run daily summary daily at midnight
@flow(schedule="0 0 * * *")
def daily_summary():
    aggregate_gold_daily_booking_summary(iceberg)
```

### 2. Conditional Execution

```python
# Skip Bronze if data already exists for this month
if not bronze_exists(extraction_month):
    bronze_extraction_flow(...)
```

### 3. Data Quality Checks

```python
# Add data quality task per entity
@task
def validate_customer_data(df: pd.DataFrame) -> bool:
    assert df['customer_id'].notna().all()
    assert len(df) > 0
    return True
```

---

## 📚 Additional Resources

- **ETL_ARCHITECTURE.md** - Original architecture documentation
- **flows_granular.py** - Granular flow implementations
- **app/etl/tasks/** - Individual task modules
- **cli_granular.py** - Enhanced CLI with entity-level control

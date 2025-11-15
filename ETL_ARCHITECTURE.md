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
│ 1. load_and_prepare_source_data (preprocessing.py)             │
│    • Load CSV file                                             │
│    • Clean columns and data                                    │
│    • Add extraction metadata (date, month, source_file)        │
│    • Return prepared DataFrame                                 │
└────────────────────┬───────────────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────────────┐
│ 2. Parallel Entity Extractions (all receive prepared_df)      │
│                                                               │
│ ┌──────────────────────────────────────────────────────────┐  │
│ │ Dimension Extractions (parallel)                         │  │
│ ├──────────────────────────────────────────────────────────┤  │
│ │ • extract_bronze_customer                                │  │
│ │ • extract_bronze_vehicle_type                            │  │
│ │ • extract_bronze_location (pickup + drop)                │  │
│ │ • extract_bronze_booking_status                          │  │
│ │ • extract_bronze_payment_method                          │  │
│ └──────────────────────────────────────────────────────────┘  │
│                                                               │
│ ┌──────────────────────────────────────────────────────────┐  │
│ │ Fact Extractions (parallel)                              │  │
│ ├──────────────────────────────────────────────────────────┤  │
│ │ • extract_bronze_booking                                 │  │
│ │ • extract_bronze_ride (completed rides only)             │  │
│ │ • extract_bronze_cancelled_ride                          │  │
│ │ • extract_bronze_incompleted_ride                        │  │
│ └──────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
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
- Each entity extraction is invoked by the **Airflow TaskFlow DAG** with `retries=2` and a 5-minute delay defined in `DEFAULT_ARGS`
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
│ Phase 2: Fact Transformations (parallel, after dims)        │
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
- Airflow TaskFlow wiring ensures dimensions finish before downstream fact tasks run

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

## 🪂 Airflow Orchestration

The canonical scheduler lives in `airflow/dags/ride_booking_medallion.py`. Three DAGs cover the standard operating modes:

- `ride_booking_medallion`: on-demand or UI-triggered runs with boolean params (`run_bronze`, `run_silver`, `run_gold`) plus overrides for `source_file`, `extraction_date`, and `no_date_filter`.
- `ride_booking_incremental`: a scheduled (2 AM) incremental loader that requires a fresh CSV path and processes Bronze → Silver → Gold.
- `ride_booking_backfill`: an on-demand DAG that skips Bronze and rematerializes Silver/Gold from existing Bronze tables.

Each DAG relies on Airflow's **TaskFlow API**, so the Python orchestration helpers in `app/etl/flows.py` are wrapped as first-class Airflow tasks with consistent retries, logging, and dependency edges:

```python
@dag(dag_id="ride_booking_medallion", schedule=None, default_args=DEFAULT_ARGS)
def ride_booking_medallion():
    @task(task_id="bronze_layer")
    def run_bronze():
        return bronze_extraction_flow(...)

    @task(task_id="silver_layer")
    def run_silver(bronze_results):
        return silver_transformation_flow(...)

    @task(task_id="gold_layer")
    def run_gold(silver_results):
        return gold_aggregation_flow(...)

    bronze_output = run_bronze()
    silver_output = run_silver(bronze_output)
    run_gold(silver_output)
```

When the Dockerized Airflow stack is running (`docker compose up` inside the `airflow/` directory), trigger runs from the UI or CLI:

```
airflow dags trigger ride_booking_medallion \
  --conf '{"source_file": "data/ncr_ride_bookings.csv", "extraction_date": "2024-12-01"}'
```

Airflow Params feed directly into the TaskFlow call stack, so the same configuration surfaces inside each layer of the medallion pipeline.

## 🔄 Python Orchestration Helpers

Airflow wraps the following functions, but they remain callable from local CLIs, notebooks, or tests when you want to bypass the scheduler.

### Main Flow

```python
from app.etl.flows import ride_booking_etl

# Run complete pipeline
results = ride_booking_etl(
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

## 📈 Performance Considerations

### Parallel Execution Speedup

Assuming 9 Bronze extractions take 1 second each:

- **Sequential (old):** 9 seconds total
- **Parallel (new):** ~1-2 seconds total (limited by CPU/IO)

Actual speedup depends on:
- Number of available Airflow workers / CPU cores
- I/O throughput (disk, network)
- Airflow executor parallelism settings (Local, Celery, etc.)

### Resource Usage

- **Memory:** Each task processes a subset of data (lower per-task memory)
- **CPU:** Better utilization through parallel execution
- **I/O:** Iceberg handles concurrent writes efficiently

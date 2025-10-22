# ETL Architecture Documentation - Ride Booking Analytics

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture Pattern](#architecture-pattern)
- [Layer Details](#layer-details)
- [Data Flow](#data-flow)
- [Date Slicing & Partitioning](#date-slicing--partitioning)
- [Running the Pipeline](#running-the-pipeline)
- [Querying Data](#querying-data)
- [Schema Reference](#schema-reference)

---

## 🎯 Overview

This ETL pipeline implements the **Medallion Architecture** (Bronze → Silver → Gold) to process ride booking data using:
- **Apache Iceberg** for ACID-compliant table storage
- **Prefect** for workflow orchestration
- **Python/Pandas** for data transformations

### Key Characteristics
- **Idempotent**: Safe to re-run multiple times
- **Partitioned**: Data organized by `extraction_month` (YYYY-MM format)
- **Versioned**: Iceberg provides time-travel capabilities
- **Incremental**: Supports processing specific date ranges

---

## 🏗️ Architecture Pattern

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE DATA                              │
│              ncr_ride_bookings.csv                          │
│                  (150,000 rows)                             │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER - Raw Data Extraction                      │
│  ─────────────────────────────────────────────────────────  │
│  • 9 tables: customer, vehicle_type, location, etc.         │
│  • Preserves raw data with metadata                         │
│  • Partitioned by extraction_month                          │
│  • Append mode (accumulates data)                           │
│  • ~650K total rows across tables                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER - Cleaned & Normalized                     │
│  ─────────────────────────────────────────────────────────  │
│  • Deduplicated dimensions (7 vehicle types, 176 locations) │
│  • Surrogate keys assigned                                  │
│  • Business metrics calculated                              │
│  • Overwrite mode (replaces with clean data)                │
│  • ~390K total rows across tables                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER - Analytics & Aggregations                   │
│  ─────────────────────────────────────────────────────────  │
│  • daily_booking_summary: 12K rows                          │
│  • customer_analytics: 148K rows                            │
│  • location_analytics: 176 rows                             │
│  • Pre-aggregated for fast querying                         │
│  • Overwrite mode (refreshes analytics)                     │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Layer Details

### 🥉 Bronze Layer - Raw Data Extraction

**Purpose**: Extract and preserve raw data from source files

| Table | Description | Row Count | Key Columns |
|-------|-------------|-----------|-------------|
| `customer` | Customer IDs from bookings | 150,000 | customer_id, booking_id, extraction_month |
| `vehicle_type` | Vehicle type per booking | 149,973 | vehicle_type_name, booking_id, extraction_month |
| `location` | Pickup & dropoff locations | 300,000 | location_name, location_type, extraction_month |
| `booking_status` | Booking status values | 149,961 | booking_status_name, booking_id, extraction_month |
| `payment_method` | Payment method per booking | 101,990 | payment_method_name, booking_id, extraction_month |
| `booking` | Main booking fact table | 149,885 | booking_id, date, time, booking_value, extraction_month |
| `ride` | Completed rides only | 92,969 | booking_id, ride_distance, ratings, extraction_month |
| `cancelled_ride` | Cancelled bookings | 37,492 | booking_id, cancelled_by, extraction_month |
| `incompleted_ride` | Incomplete rides | 8,999 | booking_id, reason, extraction_month |

**Metadata Fields Added**:
- `extraction_date`: Date when data was extracted
- `extraction_month`: Partition key (YYYY-MM format)
- `source_file`: Source CSV filename

**Write Mode**: `append` - accumulates data over time

---

### 🥈 Silver Layer - Cleaned & Normalized

**Purpose**: Transform raw data into a clean dimensional model

#### Dimension Tables (Reference Data)

| Table | Rows | Description |
|-------|------|-------------|
| `customer` | 148,678 | Unique customers with first/last seen dates, total bookings |
| `vehicle_type` | 7 | Auto, Bike, eBike, Go Mini, Go Sedan, Premier Sedan, Uber XL |
| `location` | 176 | Unique location names with assigned IDs |
| `booking_status` | 5 | Completed, Cancelled by Customer, Cancelled by Driver, Incomplete, No Driver Found |
| `payment_method` | 5 | UPI, Cash, Debit Card, Credit Card, Uber Wallet |

#### Fact Tables (Transactional Data)

| Table | Rows | Description |
|-------|------|-------------|
| `booking` | 149,885 | All bookings with foreign keys to dimensions |
| `ride` | 92,969 | Completed rides with distance and ratings |
| `cancelled_ride` | 37,492 | Cancellation details |
| `incompleted_ride` | 8,999 | Incomplete ride reasons |

**Key Transformations**:
- Deduplication of dimension data
- Surrogate key assignment (vehicle_type_id, location_id, etc.)
- Customer metrics calculation (first_seen, last_seen, total_bookings)
- Timestamp addition (created_at, updated_at)

**Write Mode**: `overwrite` - replaces tables with clean data

---

### 🥇 Gold Layer - Analytics & Aggregations

**Purpose**: Pre-aggregated analytics for business intelligence

| Table | Rows | Grain | Metrics |
|-------|------|-------|---------|
| `daily_booking_summary` | 12,022 | date + vehicle_type + status | total_bookings, total_revenue, avg_booking_value |
| `customer_analytics` | 148,678 | customer_id | total_bookings, total_spent, avg_booking_value, lifetime_days |
| `location_analytics` | 176 | location_id | pickups, dropoffs, total_activity, avg_booking_value |

**Write Mode**: `overwrite` - refreshes all analytics

---

## 🔄 Data Flow

### Complete Pipeline Execution

```python
from pathlib import Path
from datetime import datetime
from app.adapters.iceberg_adapter import IcebergAdapter
from app.etl.bronze_layer import extract_to_bronze
from app.etl.silver_layer_iceberg import transform_to_silver
from app.etl.gold_layer_iceberg import aggregate_to_gold

# Initialize adapter
iceberg = IcebergAdapter(warehouse_path="./warehouse")

# 1. Bronze: Extract from CSV
source_file = Path("data/ncr_ride_bookings.csv")
bronze_counts = extract_to_bronze(
    source_file=source_file,
    iceberg_adapter=iceberg,
    extraction_date=datetime(2024, 12, 1)
)

# 2. Silver: Transform and normalize
silver_counts = transform_to_silver(
    iceberg_adapter=iceberg,
    extraction_month="2024-12"  # Optional: process specific month
)

# 3. Gold: Aggregate analytics
gold_counts = aggregate_to_gold(
    iceberg_adapter=iceberg,
    target_date=datetime(2024, 12, 1)  # Optional: specific date
)
```

---

## 📅 Date Slicing & Partitioning

### Partition Strategy

All Bronze tables are **partitioned by `extraction_month`** (YYYY-MM format):

```python
# Data is automatically partitioned when extracted
df['extraction_month'] = df['Date'].dt.strftime('%Y-%m')
```

### Working with Date Slices

#### 1. **Extract Specific Month's Data**

```python
from datetime import datetime

# Extract only November 2024 data
extraction_date = datetime(2024, 11, 1)
bronze_counts = extract_to_bronze(
    source_file=source_file,
    iceberg_adapter=iceberg,
    extraction_date=extraction_date
)
```

#### 2. **Transform Specific Month in Silver**

```python
# Process only November 2024 data
silver_counts = transform_to_silver(
    iceberg_adapter=iceberg,
    extraction_month="2024-11"
)

# Process all months (default)
silver_counts = transform_to_silver(
    iceberg_adapter=iceberg,
    extraction_month=None
)
```

#### 3. **Query Specific Date Range**

```python
# Read Bronze data for specific month
booking_df = iceberg.read_table('bronze', 'booking')
nov_bookings = booking_df[booking_df['extraction_month'] == '2024-11']

# Query by actual booking date
import pandas as pd
booking_df['date'] = pd.to_datetime(booking_df['date'])
date_range = booking_df[
    (booking_df['date'] >= '2024-11-01') & 
    (booking_df['date'] < '2024-12-01')
]
```

#### 4. **Incremental Processing Pattern**

```python
from datetime import datetime, timedelta

def process_monthly_incremental(start_date: datetime, end_date: datetime):
    """Process data month by month incrementally"""
    current = start_date.replace(day=1)
    
    while current <= end_date:
        month_str = current.strftime('%Y-%m')
        print(f"Processing {month_str}...")
        
        # Extract for this month
        extract_to_bronze(
            source_file=source_file,
            iceberg_adapter=iceberg,
            extraction_date=current
        )
        
        # Transform for this month
        transform_to_silver(
            iceberg_adapter=iceberg,
            extraction_month=month_str
        )
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    # Final Gold aggregation (across all months)
    aggregate_to_gold(iceberg_adapter=iceberg)

# Usage
process_monthly_incremental(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)
```

#### 5. **Time Travel with Iceberg**

```python
# Query table as of specific timestamp
from pyiceberg.expressions import EqualTo

# Read historical snapshot
table = iceberg.get_table('bronze', 'booking')
historical_df = table.scan(
    snapshot_id=table.history()[0].snapshot_id
).to_pandas()

# Query with predicate pushdown (efficient filtering)
from pyiceberg.expressions import EqualTo
filtered_df = table.scan(
    row_filter=EqualTo("extraction_month", "2024-11")
).to_pandas()
```

### Date Range Analysis Examples

#### Example 1: Compare Two Months

```python
# Get bookings for two different months
booking_df = iceberg.read_table('bronze', 'booking')

oct_2024 = booking_df[booking_df['extraction_month'] == '2024-10']
nov_2024 = booking_df[booking_df['extraction_month'] == '2024-11']

print(f"October: {len(oct_2024)} bookings")
print(f"November: {len(nov_2024)} bookings")
print(f"Growth: {((len(nov_2024) - len(oct_2024)) / len(oct_2024) * 100):.1f}%")
```

#### Example 2: Quarter-over-Quarter Analysis

```python
# Get Gold analytics for Q4 2024
daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
daily_summary['date'] = pd.to_datetime(daily_summary['date'])

q4_2024 = daily_summary[
    (daily_summary['date'] >= '2024-10-01') & 
    (daily_summary['date'] < '2025-01-01')
]

q4_revenue = q4_2024.groupby('vehicle_type_name')['total_revenue'].sum()
print(q4_revenue)
```

#### Example 3: Retention Analysis

```python
# Customer retention between months
customer_analytics = iceberg.read_table('gold', 'customer_analytics')
customer_analytics['first_booking_date'] = pd.to_datetime(
    customer_analytics['first_booking_date']
)
customer_analytics['last_booking_date'] = pd.to_datetime(
    customer_analytics['last_booking_date']
)

# Customers who started in October
oct_cohort = customer_analytics[
    customer_analytics['first_booking_date'].dt.month == 10
]

# How many are still active in November?
still_active = oct_cohort[
    customer_analytics['last_booking_date'].dt.month >= 11
]

retention_rate = len(still_active) / len(oct_cohort) * 100
print(f"Retention rate: {retention_rate:.1f}%")
```

---

## 🚀 Running the Pipeline

### Option 1: Complete Pipeline (CLI)

```bash
# Run full ETL pipeline
python -m app.etl.cli run-etl \
    --source data/ncr_ride_bookings.csv \
    --warehouse ./warehouse

# Run with specific date
python -m app.etl.cli run-etl \
    --source data/ncr_ride_bookings.csv \
    --warehouse ./warehouse \
    --extraction-date 2024-11-01
```

### Option 2: Individual Layers

```bash
# Bronze only
python -m app.etl.cli extract-bronze \
    --source data/ncr_ride_bookings.csv \
    --warehouse ./warehouse

# Silver only
python -m app.etl.cli transform-silver \
    --warehouse ./warehouse \
    --extraction-month 2024-11

# Gold only
python -m app.etl.cli aggregate-gold \
    --warehouse ./warehouse
```

### Option 3: Python Script

```python
# See run_iceberg_etl.py for complete example
from app.etl.flows import run_complete_etl_flow

run_complete_etl_flow(
    source_file="data/ncr_ride_bookings.csv",
    warehouse_path="./warehouse"
)
```

## 🔍 Querying Data

### Basic Queries

```python
from app.adapters.iceberg_adapter import IcebergAdapter

iceberg = IcebergAdapter(warehouse_path="./warehouse")

# Read entire table
bookings = iceberg.read_table('bronze', 'booking')

# Check table existence
if iceberg.table_exists('silver', 'customer'):
    customers = iceberg.read_table('silver', 'customer')

# Get row count
booking_count = iceberg.get_row_count('bronze', 'booking')
print(f"Total bookings: {booking_count}")
```

### Advanced Analytics

```python
import pandas as pd

# Load Gold analytics
daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
customer_analytics = iceberg.read_table('gold', 'customer_analytics')
location_analytics = iceberg.read_table('gold', 'location_analytics')

# Top 10 customers by spend
top_customers = customer_analytics.nlargest(10, 'total_spent')

# Most popular locations
top_locations = location_analytics.nlargest(10, 'total_activity')

# Daily revenue trend
daily_summary['date'] = pd.to_datetime(daily_summary['date'])
revenue_trend = daily_summary.groupby('date')['total_revenue'].sum().sort_index()
```

### SQL-like Queries (via PyIceberg)

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, GreaterThan, And

# Load catalog
catalog = load_catalog('default', warehouse=f"file://{iceberg.warehouse_path}")

# Get table
table = catalog.load_table('silver.booking')

# Filter with expressions (pushdown to Iceberg)
filtered = table.scan(
    row_filter=And(
        EqualTo("vehicle_type_id", 1),
        GreaterThan("booking_value", 500)
    )
).to_pandas()
```

---

## 📐 Schema Reference

### Bronze Layer Schemas

All Bronze tables include these common fields:
- `extraction_date`: Date of extraction
- `extraction_month`: Partition key (YYYY-MM)
- `source_file`: Source filename

**booking** (Main fact table):
```
booking_id: string
customer_id: string
vehicle_type: string
pickup_location: string
drop_location: string
booking_status: string
payment_method: string
booking_value: float
date: date
time: string
```

### Silver Layer Schemas

**customer** (Dimension):
```
customer_id: string (PK)
first_seen_date: date
last_seen_date: date
total_bookings: long
created_at: timestamp
updated_at: timestamp
```

**vehicle_type** (Dimension):
```
vehicle_type_id: int (PK)
name: string
created_at: timestamp
updated_at: timestamp
```

**booking** (Fact):
```
booking_id: string (PK)
customer_id: string (FK)
vehicle_type_id: int (FK)
pickup_location_id: int (FK)
drop_location_id: int (FK)
booking_status_id: int (FK)
payment_method_id: int (FK)
booking_value: float
date: date
time: string
created_at: timestamp
```

### Gold Layer Schemas

**daily_booking_summary**:
```
date: date
vehicle_type_name: string
booking_status_name: string
total_bookings: long
total_revenue: double
avg_booking_value: double
created_at: timestamp
updated_at: timestamp
```

**customer_analytics**:
```
customer_id: string
first_seen_date: date
last_seen_date: date
total_bookings: long
total_spent: double
avg_booking_value: double
customer_lifetime_days: int
created_at: timestamp
updated_at: timestamp
```

---

## 🛠️ Configuration

### Environment Settings

Edit `app/config/settings.py`:

```python
WAREHOUSE_PATH = Path("./warehouse")
DATA_PATH = Path("./data")
LOG_LEVEL = "INFO"
```

### Iceberg Configuration

Tables are created with:
- Format: Iceberg v2
- Partitioning: `extraction_month` (Bronze only)
- File format: Parquet (default)
- ACID transactions: Enabled

---

## 🔧 Troubleshooting

### Common Issues

**Q: Bronze tables keep growing**  
A: This is expected. Bronze uses `append` mode to preserve history. Use `extraction_month` filters.

**Q: Silver has fewer rows than Bronze**  
A: This is correct. Silver deduplicates dimension data (e.g., 150K customers → 148K unique).

**Q: Gold numbers don't match Silver**  
A: Gold aggregates data. Check the aggregation grain (e.g., daily summaries vs. individual bookings).

**Q: How to reprocess a specific month?**  
A: Use `extraction_month` parameter in `transform_to_silver()` or manually delete and re-extract.

---

## 📚 Additional Resources

- **ETL_README.md** - Usage guide and examples
- **ICEBERG_README.md** - Apache Iceberg details
- **QUICKSTART.md** - Quick start guide
- **README.md** - Project overview

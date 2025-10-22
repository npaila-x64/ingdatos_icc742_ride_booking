# Ride Booking ETL with Apache Iceberg

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) ETL pipeline for ride booking data using **Apache Iceberg** with **Parquet files** and a **filesystem/Hadoop catalog** - all running locally without Docker.

## Architecture Overview

### Medallion Layers

1. **Bronze Layer**: Raw data ingestion from CSV files into Iceberg tables (Parquet format)
2. **Silver Layer**: Cleaned and normalized dimensional model
3. **Gold Layer**: Aggregated analytics-ready tables

### Storage: Apache Iceberg

- **Format**: Parquet files
- **Catalog**: Hadoop filesystem catalog (local)
- **Warehouse Location**: `./warehouse/` (configurable via `ICEBERG_WAREHOUSE`)
- **Benefits**: 
  - ACID transactions
  - Schema evolution
  - Time travel capabilities
  - Efficient columnar storage with Parquet
  - No database server required

## Prerequisites

- Python 3.10+
- Virtual environment (recommended)

## Installation

1. **Create and activate a virtual environment:**

```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
venv\Scripts\activate  # On Windows
```

2. **Install the package:**

```bash
pip install -e .
```

## Configuration

The project uses environment variables for configuration. Create a `.env` file in the project root:

```bash
# Project paths
PROJECT_BASE_PATH=.
PROJECT_DATA_DIR=data

# Iceberg warehouse (where tables are stored)
ICEBERG_WAREHOUSE=warehouse

# Prefect settings (optional)
PREFECT_PROFILE=ride-booking-local
```

## Usage

### Running the ETL Pipeline

**Option 1: Use the test script**

```bash
python run_iceberg_etl.py
```

**Option 2: Use the Prefect flow directly**

```bash
python -m app.etl.flows
```

**Option 3: Use the CLI (if configured)**

```bash
ride-booking-etl
```

### Running Specific Layers

```python
from app.etl.flows import ride_booking_etl

# Run only Bronze layer
results = ride_booking_etl(run_bronze=True, run_silver=False, run_gold=False)

# Run Silver and Gold (skip Bronze)
results = ride_booking_etl(run_bronze=False, run_silver=True, run_gold=True)
```

### Custom Source File

```python
from app.etl.flows import ride_booking_etl

results = ride_booking_etl(
    source_file="path/to/your/data.csv",
    extraction_date=datetime(2024, 1, 15)
)
```

## Data Structure

### Warehouse Directory

After running the pipeline, you'll have the following structure:

```
warehouse/
├── bronze.db/
│   ├── customer/
│   │   ├── data/
│   │   │   └── *.parquet
│   │   └── metadata/
│   ├── booking/
│   ├── vehicle_type/
│   └── ...
├── silver.db/
│   ├── customer/
│   ├── booking/
│   └── ...
└── gold.db/
    ├── daily_booking_summary/
    ├── customer_analytics/
    └── location_analytics/
```

### Querying Iceberg Tables

You can read the Iceberg tables using PyIceberg or any Iceberg-compatible tool:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    **{
        "type": "hadoop",
        "warehouse": "warehouse",
    }
)

# List tables
tables = catalog.list_tables("bronze")
print(tables)

# Read a table
table = catalog.load_table("bronze.customer")
df = table.scan().to_pandas()
print(df.head())
```

### Using with DuckDB

DuckDB has excellent Iceberg support and can query the tables directly:

```python
import duckdb

con = duckdb.connect()
con.install_extension("iceberg")
con.load_extension("iceberg")

# Query Iceberg table
df = con.execute("""
    SELECT * FROM iceberg_scan('warehouse/bronze.db/customer')
    LIMIT 10
""").df()
```

## Schema Definitions

All Iceberg table schemas are defined in `app/adapters/iceberg_schemas.py`. Each table has a strongly-typed schema with:

- Data types (String, Integer, Double, Date, Timestamp, etc.)
- Required vs optional fields
- Partitioning specifications (where applicable)

## Key Differences from PostgreSQL Version

### Before (PostgreSQL):
- Required PostgreSQL server (Docker or local)
- SQL-based transformations
- Database connection management
- SQL upsert operations

### After (Apache Iceberg):
- No server required - pure filesystem storage
- Pandas-based transformations
- Direct file I/O via PyIceberg
- DataFrame-based operations
- ACID guarantees via Iceberg metadata

### Migration Notes:

1. **No SQL Queries**: All transformations use pandas DataFrames
2. **No Connection Pooling**: Direct filesystem access
3. **Simpler Deployment**: No database container/service needed
4. **Better Performance**: Columnar Parquet format optimized for analytics
5. **Schema Evolution**: Iceberg handles schema changes gracefully

## ETL Layers

### Bronze Layer (`app/etl/bronze_layer_iceberg.py`)

Extracts raw data from CSV and writes to Iceberg tables:

- `bronze.customer`
- `bronze.vehicle_type`
- `bronze.location`
- `bronze.booking_status`
- `bronze.payment_method`
- `bronze.booking`
- `bronze.ride`
- `bronze.cancelled_ride`
- `bronze.incompleted_ride`

### Silver Layer (`app/etl/silver_layer_iceberg.py`)

Transforms Bronze data into normalized dimensions and facts:

**Dimensions:**
- `silver.customer`
- `silver.vehicle_type`
- `silver.location`
- `silver.booking_status`
- `silver.payment_method`

**Facts:**
- `silver.booking`
- `silver.ride`
- `silver.cancelled_ride`
- `silver.incompleted_ride`

### Gold Layer (`app/etl/gold_layer_iceberg.py`)

Aggregates Silver data for analytics:

- `gold.daily_booking_summary`
- `gold.customer_analytics`
- `gold.location_analytics`

## Development

### Project Structure

```
app/
├── adapters/
│   ├── iceberg_adapter.py       # Iceberg operations wrapper
│   └── iceberg_schemas.py       # Table schema definitions
├── config/
│   └── settings.py              # Configuration management
├── etl/
│   ├── bronze_layer_iceberg.py  # Bronze extraction
│   ├── silver_layer_iceberg.py  # Silver transformation
│   ├── gold_layer_iceberg.py    # Gold aggregation
│   └── flows.py                 # Prefect orchestration
└── tools/
```

### Adding New Tables

1. Define schema in `app/adapters/iceberg_schemas.py`
2. Create extraction/transformation function in appropriate layer file
3. Update main flow in `app/etl/flows.py`

Example:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType

NEW_TABLE_SCHEMA = Schema(
    NestedField(1, "id", StringType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "count", IntegerType(), required=False),
)
```

## Advantages of Iceberg + Parquet

1. **No Infrastructure**: No database server to manage
2. **Portability**: Files can be moved/backed up easily
3. **Interoperability**: Read with Spark, Trino, Athena, DuckDB, etc.
4. **Performance**: Columnar format optimized for analytics
5. **Time Travel**: Query historical versions of data
6. **ACID**: Safe concurrent reads/writes
7. **Schema Evolution**: Add/modify columns without rewriting data
8. **Partitioning**: Efficient filtering on date/time columns

## Troubleshooting

### Import Errors

Ensure you're in the virtual environment:
```bash
source venv/bin/activate  # Linux/Mac
```

### PyIceberg Not Found

```bash
pip install pyiceberg pyarrow
```

### Warehouse Directory Permissions

Ensure the warehouse directory is writable:
```bash
chmod -R 755 warehouse/
```

### Checking Table Contents

```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

settings = load_settings()
adapter = IcebergAdapter(settings.iceberg)

# List namespaces
print(adapter.catalog.list_namespaces())

# Read a table
df = adapter.read_table('bronze', 'customer')
print(df.head())
```

## Future Enhancements

- [ ] Add Iceberg metadata pruning for better query performance
- [ ] Implement incremental processing with Iceberg snapshots
- [ ] Add time travel queries for auditing
- [ ] Integrate with Apache Spark for distributed processing
- [ ] Add DuckDB integration for SQL analytics
- [ ] Implement Iceberg table compaction
- [ ] Add monitoring/metrics dashboard

## License

MIT License

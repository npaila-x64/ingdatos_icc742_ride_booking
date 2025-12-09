# ETL Pipeline Overview

## Architecture

**Medallion Architecture** (Bronze → Silver → Gold) processing ride booking data using **Apache Iceberg** tables and **Prefect** orchestration.

## Data Flow

```
CSV Source → Bronze (raw) → Silver (normalized) → Gold (aggregated)
```

### Bronze Layer
- **Purpose**: Raw data extraction
- **Process**: Load CSV → Clean → Extract 9 entities in parallel
- **Entities**: Customer, Vehicle Type, Location, Booking Status, Payment Method, Booking, Ride, Cancelled Ride, Incompleted Ride
- **Storage**: Append-only Iceberg tables

### Silver Layer  
- **Purpose**: Normalized dimensional model
- **Process**: Transform bronze → Deduplicate → Assign surrogate keys → Enrich facts
- **Phase 1**: Transform dimensions (parallel)
- **Phase 2**: Transform facts with dimension lookups (parallel, after Phase 1)
- **Storage**: Upsert to Iceberg tables

### Gold Layer
- **Purpose**: Business analytics aggregations
- **Tables**: Daily Booking Summary, Customer Analytics, Location Analytics
- **Storage**: Replace or upsert to Iceberg tables

## Key Technologies

- **Storage**: Apache Iceberg (ACID transactions, time travel, schema evolution)
- **Orchestration**: Prefect (flows, tasks, retries, parallel execution)
- **Catalog**: SQLite-backed filesystem catalog
- **Processing**: Pandas + PyArrow
- **Visualization**: Streamlit dashboards (analytics + SQL query interface)

## Task Design

- **Granular**: One task per entity/table
- **Idempotent**: Safe to re-run
- **Parallel**: Independent tasks execute concurrently
- **Resilient**: Automatic retries (2x with 30s delay)

## Entry Points

- **ETL**: `run_iceberg_etl.py` - Run full pipeline
- **CLI**: `app/etl/cli.py` - Run specific layers/tables
- **Dashboard**: `./run_dashboard.sh` - Analytics visualization
- **Query**: `./run_sql_query.sh` - SQL query interface

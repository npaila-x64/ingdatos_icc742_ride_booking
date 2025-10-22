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

- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide
- **[ETL_ARCHITECTURE.md](ETL_ARCHITECTURE.md)** - Architecture details
- **[streamlit_apps/](streamlit_apps/)** - Web visualization dashboards

## 🎨 Visualization & Querying

Interactive web-based tools for exploring your Iceberg data:

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

# ETL Orchestrator - Airflow Setup

This directory contains the Apache Airflow 3.1.3 project for orchestrating the ride booking ETL pipeline.

## Quick Start

### 1. Start Airflow (Standalone Mode - Recommended for Development)

```bash
cd etl_orchestrator
./start_standalone.sh
```

This will:
- Start webserver, scheduler, triggerer, and dag-processor in one process
- Create an admin user automatically
- Display the admin password in the console

**Access the UI**: http://localhost:8080

**Credentials**: The password will be displayed when you start Airflow. Look for:
```
Simple auth manager | Password for user 'admin': <YOUR_PASSWORD>
```

### 2. Alternative: Start Services Separately

For more control, you can start services individually:

```bash
./start_airflow.sh     # Starts webserver, scheduler, triggerer in background
./view_logs.sh         # View logs (all, webserver, scheduler, or triggerer)
./stop_airflow.sh      # Stop all services
```

### 3. Re-run Setup (if needed)

If you need to reinstall or update dependencies:

```bash
./setup_airflow.sh
```

## Project Structure

```
etl_orchestrator/
├── dags/                      # DAG definitions
│   ├── etl/                   # ETL workflow DAGs
│   │   └── __init__.py
│   ├── utils/                 # Shared utilities
│   │   └── __init__.py
│   └── ride_booking_etl_example.py  # Example DAG
├── plugins/                   # Custom operators/hooks
│   └── __init__.py
├── tests/                     # Unit tests
├── .env                       # Environment variables
├── .gitignore
├── requirements.txt           # Python dependencies
└── settings.yaml             # airflowctl configuration
```

## Development Workflow

1. **Create/Edit DAGs**: Add or modify files in `dags/`
2. **Add Dependencies**: Update `requirements.txt` and rebuild:
   ```bash
   airflowctl build
   ```
3. **Test DAGs**: Use the Airflow UI at http://localhost:8080
4. **Run Tests**: 
   ```bash
   pytest tests/
   ```

## Connecting to Your Existing ETL

To integrate with your existing ETL code in `app/etl/`, you can:

1. Import modules directly in your DAGs:
   ```python
   import sys
   sys.path.insert(0, '/path/to/ingdatos_icc742_ride_booking')
   from app.etl.flows import run_bronze_layer
   ```

2. Or package your ETL code and install it:
   ```bash
   pip install -e /path/to/ingdatos_icc742_ride_booking
   ```

## Configuration

### Environment Variables (`.env`)

Key settings:
- `AIRFLOW__CORE__EXECUTOR`: Executor type (SequentialExecutor for dev)
- `AIRFLOW__CORE__DAGS_FOLDER`: Path to DAGs directory
- `AIRFLOW__LOGGING__BASE_LOG_FOLDER`: Path to logs

### Connections & Variables (`settings.yaml`)

Predefined connections and variables are automatically loaded from `settings.yaml`.

## Using the Airflow CLI

Run Airflow commands through `airflowctl`:

```bash
# List DAGs
airflowctl airflow dags list

# Test a task
airflowctl airflow tasks test ride_booking_etl_example bronze_extraction 2024-03-01

# Show DAG structure
airflowctl airflow dags show ride_booking_etl_example
```

# Quick Start Guide - Ride Booking ETL

Get up and running with the Ride Booking ETL pipeline in 5 minutes.

## 1. Prerequisites Check

```bash
# Check Python version (requires 3.10+)
python --version

# Check Docker (recommended)
docker --version
docker-compose --version
```

## 2. Setup (Choose One Method)

### Method A: Docker (Easiest)

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Wait for database to be ready (~10 seconds)
docker-compose ps

# You should see postgres as "healthy"
```

### Method B: Local Python

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Ensure PostgreSQL is running locally
# Update .env with your database credentials
cp .env.example .env
# Edit .env file with your DB settings
```

## 3. Run Your First ETL

```bash
# Using Python directly (works for both methods)
python -m app.etl.cli run

# Or using make (if Docker)
make etl-run
```

Expected output:
```
================================================================================
BRONZE LAYER: Extracting raw data
================================================================================
Loaded 150,001 rows from source file
Bronze extraction completed: 150,000+ total rows

================================================================================
SILVER LAYER: Transforming to dimensional model
================================================================================
Transformed X customers to Silver
Transformed X bookings to Silver
...

================================================================================
GOLD LAYER: Aggregating analytics
================================================================================
Aggregated X daily summaries to Gold
...

================================================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
================================================================================
```

## 4. Verify Results

### Option A: Command Line

```bash
# Query database directly
docker-compose exec postgres psql -U postgres -d ride_booking

# Then run SQL:
SELECT * FROM gold.daily_booking_summary ORDER BY summary_date DESC LIMIT 5;
```

### Option B: pgAdmin (GUI)

```bash
# Start pgAdmin
docker-compose --profile dev up -d pgadmin

# Open browser: http://localhost:5050
# Login: admin@ridebooking.com / admin

# Add server:
# Host: postgres
# Port: 5432
# User: postgres
# Password: postgres
```

### Option C: Python Query

```python
from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings

settings = load_settings()
adapter = PostgreSQLAdapter(settings.database)

# Query Gold layer
results = adapter.execute_query("""
    SELECT summary_date, total_bookings, total_revenue 
    FROM gold.daily_booking_summary 
    ORDER BY summary_date DESC 
    LIMIT 5
""")

for row in results:
    print(f"{row['summary_date']}: {row['total_bookings']} bookings, ${row['total_revenue']}")

adapter.close()
```

## 5. Explore the Data

### Check Layer Statistics

```bash
# Bronze layer
make db-query-bronze

# Silver layer
make db-query-silver

# Gold layer
make db-query-gold
```

### Sample Queries

```sql
-- Top 10 customers by total bookings
SELECT 
    customer_id, 
    total_bookings, 
    total_spent,
    avg_rating
FROM gold.customer_analytics
ORDER BY total_bookings DESC
LIMIT 10;

-- Busiest locations (by pickups)
SELECT 
    location_name,
    total_pickups,
    total_drops
FROM gold.location_analytics
ORDER BY total_pickups DESC
LIMIT 10;

-- Daily revenue trend
SELECT 
    summary_date,
    total_bookings,
    total_revenue,
    avg_ride_distance
FROM gold.daily_booking_summary
ORDER BY summary_date DESC;
```

## 6. Next Steps

### Run Example with Diagnostics

```bash
python examples/run_etl_example.py
```

This will:
- ✓ Check database connection
- ✓ Verify schemas
- ✓ Count source rows
- ✓ Run ETL pipeline
- ✓ Query sample data from each layer

### Explore Different Workflows

```bash
# Incremental load (for new data files)
python -m app.etl.cli incremental --source-file data/new_bookings.csv

# Backfill (reprocess Silver and Gold)
python -m app.etl.cli backfill

# Run with specific date
python -m app.etl.cli run --extraction-date 2024-10-21

# Skip layers
python -m app.etl.cli run --skip-bronze --skip-gold
```

### View Logs

```bash
# All services
docker-compose logs -f

# Just database
docker-compose logs -f postgres

# Just ETL app
docker-compose logs -f etl_app
```

## Common Issues & Solutions

### Issue: Database connection refused

**Solution:**
```bash
# Check if postgres is running
docker-compose ps

# Restart if needed
docker-compose restart postgres

# Check logs
docker-compose logs postgres
```

### Issue: Source file not found

**Solution:**
```bash
# Check file exists
ls -lh data/ncr_ride_bookings.csv

# Use absolute path
python -m app.etl.cli run --source-file /full/path/to/data.csv
```

### Issue: Import errors

**Solution:**
```bash
# Reinstall dependencies
pip install -e .

# Or in Docker
docker-compose restart etl_app
```

## Help & Documentation

- **Full ETL Documentation**: [ETL_README.md](ETL_README.md)
- **Docker Guide**: [DOCKER.md](DOCKER.md)
- **Main README**: [README.md](README.md)
- **Examples**: [examples/run_etl_example.py](examples/run_etl_example.py)

### Get Help

```bash
# CLI help
python -m app.etl.cli --help

# Makefile commands
make help

# Database help
docker-compose exec postgres psql --help
```

## Summary

You've now:
1. ✅ Started the database
2. ✅ Run the ETL pipeline
3. ✅ Verified data in Bronze/Silver/Gold layers
4. ✅ Queried analytics results

**Your data warehouse is ready!** 🎉

Next, explore the [ETL_README.md](ETL_README.md) for advanced features like:
- Prefect orchestration and scheduling
- Custom transformations
- Performance tuning
- Production deployment

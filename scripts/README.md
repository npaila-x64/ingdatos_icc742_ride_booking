# ETL Scripts Directory

This directory contains utility scripts for running and managing the Ride Booking ETL pipeline.

## 📂 Directory Structure

```
scripts/
├── README.md                    # This file
├── run_etl_for_date.py         # Generic ETL runner for any date
├── check_location.py           # Utility to inspect location table structure
└── date_slices/                # Date-specific ETL runs and reports
    ├── run_march_2024_etl.py
    ├── verify_march_2024.py
    └── MARCH_2024_ETL_SUMMARY.md
```

---

## 🚀 Quick Start

### Run ETL for a Specific Month

```bash
# March 2024
python scripts/run_etl_for_date.py --year 2024 --month 3

# June 2024
python scripts/run_etl_for_date.py --year 2024 --month 6

# Any month with specific day
python scripts/run_etl_for_date.py --year 2024 --month 7 --day 15
```

### Run a Date-Specific Script

```bash
# Run March 2024 ETL
python scripts/date_slices/run_march_2024_etl.py

# Verify March 2024 results
python scripts/date_slices/verify_march_2024_etl.py
```

---

## 📜 Script Reference

### `run_etl_for_date.py`
**Purpose**: Generic ETL runner for any date or month  
**Usage**:
```bash
python scripts/run_etl_for_date.py --year YYYY --month MM [--day DD]
```

**Parameters**:
- `--year`: Year (e.g., 2024) - **Required**
- `--month`: Month (1-12) - **Required**
- `--day`: Day (1-31) - Optional, defaults to 1

**Example**:
```bash
# Run for April 2024
python scripts/run_etl_for_date.py --year 2024 --month 4
```

**What it does**:
1. Extracts data from `data/ncr_ride_bookings.csv`
2. Runs Bronze → Silver → Gold transformations
3. Partitions Bronze data by `extraction_month`
4. Displays summary statistics

---

### `check_location.py`
**Purpose**: Inspect the structure and content of the location table  
**Usage**:
```bash
python scripts/check_location.py
```

**Output**: Displays location table columns, shape, and sample rows

---

## 📅 Date Slices Directory

The `date_slices/` subdirectory contains month-specific ETL scripts and reports. Each date slice typically includes:

1. **`run_<month>_<year>_etl.py`** - Execution script for that specific month
2. **`verify_<month>_<year>.py`** - Verification and analysis script
3. **`<MONTH>_<YEAR>_ETL_SUMMARY.md`** - Detailed summary report

### Creating a New Date Slice

**Option 1: Use the generic script**
```bash
python scripts/run_etl_for_date.py --year 2024 --month 5
```

**Option 2: Create a dedicated script** (for complex/recurring needs)

1. Copy the template from an existing date slice:
   ```bash
   cp scripts/date_slices/run_march_2024_etl.py scripts/date_slices/run_may_2024_etl.py
   ```

2. Edit the dates in the new script:
   ```python
   # Change this line
   extraction_date = datetime(2024, 5, 1)  # May 2024
   ```

3. Run the new script:
   ```bash
   python scripts/date_slices/run_may_2024_etl.py
   ```

4. Verify the results:
   ```bash
   cp scripts/date_slices/verify_march_2024.py scripts/date_slices/verify_may_2024.py
   # Edit the verification script to check for '2024-05' partition
   python scripts/date_slices/verify_may_2024.py
   ```

---

## 🔄 Common Workflows

### 1. Run ETL for Multiple Months

```bash
# Process Q1 2024 (Jan, Feb, Mar)
for month in 1 2 3; do
    python scripts/run_etl_for_date.py --year 2024 --month $month
done

# Process entire year
for month in {1..12}; do
    python scripts/run_etl_for_date.py --year 2024 --month $month
done
```

### 2. Incremental Monthly Processing

```python
# Create a custom script for incremental processing
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.etl.flows import granular_ride_booking_etl

# Process multiple months
months = [
    (2024, 1),  # January
    (2024, 2),  # February
    (2024, 3),  # March
]

for year, month in months:
    print(f"\nProcessing {year}-{month:02d}...")
    extraction_date = datetime(year, month, 1)
    
    granular_ride_booking_etl(
        source_file="data/ncr_ride_bookings.csv",
        extraction_date=extraction_date,
        run_bronze=True,
        run_silver=True,
        run_gold=True,
    )
```

### 3. Reprocess a Specific Month

If you need to reprocess data for a month (e.g., after fixing data issues):

```bash
# Step 1: Run ETL for the month
python scripts/run_etl_for_date.py --year 2024 --month 3

# Step 2: Verify the results
python scripts/date_slices/verify_march_2024.py
```

**Note**: Bronze layer uses `append` mode, so re-running will add duplicate data. Consider:
- Using Silver/Gold reprocessing only (set `run_bronze=False`)
- Or manually clearing Bronze tables before re-running

---

## 📊 Verification and Analysis

After running ETL for any date slice, you can query the results:

```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings
import pandas as pd

# Initialize
settings = load_settings()
iceberg = IcebergAdapter(settings.iceberg)

# Check what partitions exist
bookings = iceberg.read_table('bronze', 'booking')
partitions = bookings['extraction_month'].value_counts().sort_index()
print("Available partitions:")
print(partitions)

# Query specific month
march_data = bookings[bookings['extraction_month'] == '2024-03']
print(f"\nMarch 2024: {len(march_data):,} bookings")

# Get analytics for March
daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
daily_summary['date'] = pd.to_datetime(daily_summary['date'])
march_analytics = daily_summary[
    (daily_summary['date'] >= '2024-03-01') & 
    (daily_summary['date'] < '2024-04-01')
]
print(f"March revenue: ₹{march_analytics['total_revenue'].sum():,.2f}")
```

---

## 🛠️ Troubleshooting

### Script Can't Find Modules

Make sure you're running scripts from the project root:
```bash
# From project root
python scripts/run_etl_for_date.py --year 2024 --month 3

# NOT from scripts directory
cd scripts  # ❌ Don't do this
python run_etl_for_date.py --year 2024 --month 3  # ❌ Will fail
```

Or activate the virtual environment:
```bash
source venv/bin/activate
python scripts/run_etl_for_date.py --year 2024 --month 3
```

### Duplicate Data in Bronze Layer

Bronze uses `append` mode. If you run the same month twice, you'll get duplicates.

**Solutions**:
1. Only reprocess Silver/Gold (they use `overwrite` mode)
2. Delete Bronze tables before re-running
3. Use extraction_month filters when querying

### Verify Data After ETL

Always verify after running ETL:
```bash
# Check table sizes
python scripts/check_location.py

# Or use a verification script
python scripts/date_slices/verify_march_2024.py
```

---

## 📝 Best Practices

1. **Use the generic script** (`run_etl_for_date.py`) for one-off runs
2. **Create dedicated scripts** in `date_slices/` for:
   - Recurring monthly processes
   - Complex analysis requirements
   - Documentation purposes

3. **Always verify** results after ETL runs
4. **Document** significant runs with summary reports (see `MARCH_2024_ETL_SUMMARY.md`)
5. **Use consistent naming**:
   - Scripts: `run_<month>_<year>_etl.py`
   - Verification: `verify_<month>_<year>.py`
   - Reports: `<MONTH>_<YEAR>_ETL_SUMMARY.md`

---

## 🔗 Related Documentation

- **`ETL_ARCHITECTURE.md`** - Detailed architecture and design patterns
- **`QUICKSTART.md`** - Getting started guide
- **`README.md`** - Project overview
- **`app/etl/cli.py`** - CLI interface for ETL operations

---

## 📞 Support

For issues or questions:
1. Check the main project documentation
2. Review existing date slice scripts as examples
3. Inspect the `app/etl/` modules for implementation details

---

**Last Updated**: October 22, 2025  
**Maintained By**: ETL Development Team

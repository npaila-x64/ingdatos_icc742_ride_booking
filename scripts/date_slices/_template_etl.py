#!/usr/bin/env python3
"""
Template for creating date-specific ETL runs.

To use this template:
1. Copy this file to a new script: cp scripts/date_slices/_template_etl.py scripts/date_slices/run_<month>_<year>_etl.py
2. Update the YEAR, MONTH, and DAY variables below
3. Update the print statements to reflect your date range
4. Run the script: python scripts/date_slices/run_<month>_<year>_etl.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.etl.flows import granular_ride_booking_etl

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES FOR YOUR DATE SLICE
# ============================================================================
YEAR = 2024
MONTH = 1  # 1-12
DAY = 1    # Usually 1 for the first day of the month

# Optional: Set custom month name for display
MONTH_NAME = datetime(YEAR, MONTH, DAY).strftime('%B %Y')  # e.g., "January 2024"
# ============================================================================


if __name__ == "__main__":
    print("=" * 80)
    print(f"RIDE BOOKING ETL - {MONTH_NAME.upper()}")
    print(f"Date Range: {MONTH_NAME} (All dates in the month)")
    print("=" * 80)
    print()
    
    # Set extraction date
    extraction_date = datetime(YEAR, MONTH, DAY)
    extraction_month = extraction_date.strftime('%Y-%m')
    
    print(f"Extraction Date: {extraction_date.date()}")
    print(f"Extraction Month: {extraction_month}")
    print()
    
    # Run the complete ETL pipeline
    results = granular_ride_booking_etl(
        source_file="data/ncr_ride_bookings.csv",
        extraction_date=extraction_date,
        run_bronze=True,   # Set to False if you want to skip Bronze extraction
        run_silver=True,   # Set to False if you want to skip Silver transformation
        run_gold=True,     # Set to False if you want to skip Gold aggregation
    )
    
    print("\n" + "=" * 80)
    print(f"ETL PIPELINE COMPLETED FOR {MONTH_NAME.upper()}!")
    print("=" * 80)
    print("\nResults Summary:")
    for layer, counts in results.items():
        print(f"\n{layer.upper()} Layer:")
        for table, count in counts.items():
            print(f"  ✓ {table}: {count:,} rows")
    
    print("\n" + "=" * 80)
    print("Next Steps:")
    print("=" * 80)
    print(f"1. Verify data in warehouse/bronze/ (partitioned by extraction_month='{extraction_month}')")
    print("2. Check Silver layer for normalized dimensions and facts")
    print(f"3. Review Gold layer analytics for {MONTH_NAME} insights")
    print("\nTo query the data:")
    print("  from app.adapters.iceberg_adapter import IcebergAdapter")
    print("  from app.config.settings import load_settings")
    print("  settings = load_settings()")
    print("  iceberg = IcebergAdapter(settings.iceberg)")
    print("  df = iceberg.read_table('bronze', 'booking')")
    print(f"  {extraction_month.replace('-', '_')}_data = df[df['extraction_month'] == '{extraction_month}']")
    print()

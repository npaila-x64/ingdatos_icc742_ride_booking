#!/usr/bin/env python3
"""Run ETL pipeline for March 2024 data slice (March 1-31, 2024)."""

import sys
from pathlib import Path
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.etl.flows import ride_booking_etl

if __name__ == "__main__":
    print("=" * 80)
    print("RIDE BOOKING ETL - MARCH 2024 DATA SLICE")
    print("Date Range: March 1, 2024 - March 31, 2024")
    print("=" * 80)
    print()
    
    # Set extraction date to March 1, 2024
    extraction_date = datetime(2024, 3, 1)
    
    print(f"Extraction Date: {extraction_date.date()}")
    print(f"Extraction Month: {extraction_date.strftime('%Y-%m')}")
    print()
    
    # Run the complete ETL pipeline
    results = ride_booking_etl(
        source_file="data/ncr_ride_bookings.csv",
        extraction_date=extraction_date,
        run_bronze=True,
        run_silver=True,
        run_gold=True,
    )
    
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETED FOR MARCH 2024!")
    print("=" * 80)
    print("\nResults Summary:")
    for layer, counts in results.items():
        print(f"\n{layer.upper()} Layer:")
        for table, count in counts.items():
            print(f"  ✓ {table}: {count:,} rows")
    
    print("\n" + "=" * 80)
    print("Next Steps:")
    print("=" * 80)
    print("1. Verify data in warehouse/bronze/ (partitioned by extraction_month='2024-03')")
    print("2. Check Silver layer for normalized dimensions and facts")
    print("3. Review Gold layer analytics for March 2024 insights")
    print("\nTo query the data:")
    print("  from app.adapters.iceberg_adapter import IcebergAdapter")
    print("  iceberg = IcebergAdapter(warehouse_path='./warehouse')")
    print("  df = iceberg.read_table('bronze', 'booking')")
    print("  march_data = df[df['extraction_month'] == '2024-03']")
    print()

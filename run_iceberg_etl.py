#!/usr/bin/env python3
"""Test script to run the Iceberg-based ETL pipeline."""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.etl.flows import granular_ride_booking_etl

if __name__ == "__main__":
    print("Starting Iceberg-based ETL pipeline...")
    print("=" * 80)
    
    results = granular_ride_booking_etl()
    
    print("\n" + "=" * 80)
    print("ETL Pipeline Completed!")
    print("=" * 80)
    print("\nResults:")
    for layer, counts in results.items():
        print(f"\n{layer.upper()}:")
        for table, count in counts.items():
            print(f"  {table}: {count} rows")

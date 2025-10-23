#!/usr/bin/env python3
"""
Generic ETL runner for any date range.

Usage examples:
    # Run for a specific month
    python run_etl_for_date.py --year 2024 --month 6

    # Run for a specific date
    python run_etl_for_date.py --year 2024 --month 6 --day 15
    
    # Or edit the script to set dates programmatically
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.etl.flows import ride_booking_etl


def run_etl_for_date(year: int, month: int, day: int = 1):
    """Run ETL pipeline for a specific date.
    
    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        day: Day (1-31), defaults to 1st of the month
    """
    extraction_date = datetime(year, month, day)
    month_str = extraction_date.strftime('%Y-%m')
    
    print("=" * 80)
    print(f"RIDE BOOKING ETL - {extraction_date.strftime('%B %Y').upper()}")
    print(f"Date: {extraction_date.date()}")
    print(f"Extraction Month: {month_str}")
    print("=" * 80)
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
    print(f"ETL PIPELINE COMPLETED FOR {extraction_date.strftime('%B %Y').upper()}!")
    print("=" * 80)
    print("\nResults Summary:")
    
    total_rows = 0
    for layer, counts in results.items():
        layer_total = sum(counts.values())
        total_rows += layer_total
        print(f"\n{layer.upper()} Layer ({layer_total:,} total rows):")
        for table, count in counts.items():
            print(f"  ✓ {table}: {count:,} rows")
    
    print(f"\n{'=' * 80}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Data stored in: ./warehouse/")
    print(f"Partition: extraction_month='{month_str}'")
    print(f"{'=' * 80}")
    print()
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Run ETL pipeline for a specific date or month'
    )
    parser.add_argument(
        '--year', 
        type=int, 
        required=True,
        help='Year (e.g., 2024)'
    )
    parser.add_argument(
        '--month', 
        type=int, 
        required=True,
        help='Month (1-12)'
    )
    parser.add_argument(
        '--day', 
        type=int, 
        default=1,
        help='Day (1-31), defaults to 1'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not (1 <= args.month <= 12):
        print("❌ Error: Month must be between 1 and 12")
        sys.exit(1)
    
    if not (1 <= args.day <= 31):
        print("❌ Error: Day must be between 1 and 31")
        sys.exit(1)
    
    try:
        run_etl_for_date(args.year, args.month, args.day)
        print("✅ ETL completed successfully!")
    except Exception as e:
        print(f"❌ Error running ETL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # If no command-line args, you can uncomment and edit this for quick testing:
    # run_etl_for_date(year=2024, month=3, day=1)
    
    # Otherwise, parse command-line arguments
    main()

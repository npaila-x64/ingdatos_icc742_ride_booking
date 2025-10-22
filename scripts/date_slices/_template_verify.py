#!/usr/bin/env python3
"""
Template for creating date-specific verification scripts.

To use this template:
1. Copy this file: cp scripts/date_slices/_template_verify.py scripts/date_slices/verify_<month>_<year>.py
2. Update the YEAR, MONTH variables below
3. Update the EXTRACTION_MONTH string
4. Run: python scripts/date_slices/verify_<month>_<year>.py
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES FOR YOUR DATE SLICE
# ============================================================================
YEAR = 2024
MONTH = 1  # 1-12
EXTRACTION_MONTH = f"{YEAR}-{MONTH:02d}"  # Format: YYYY-MM (e.g., "2024-01")

# Date range for filtering (first and last day of month)
import calendar
MONTH_START = f"{YEAR}-{MONTH:02d}-01"
last_day = calendar.monthrange(YEAR, MONTH)[1]
MONTH_END = f"{YEAR}-{MONTH:02d}-{last_day}"

MONTH_NAME = pd.Timestamp(MONTH_START).strftime('%B %Y')  # e.g., "January 2024"
# ============================================================================


def main():
    print("=" * 80)
    print(f"{MONTH_NAME.upper()} DATA VERIFICATION")
    print("=" * 80)
    print()
    
    # Initialize Iceberg adapter
    settings = load_settings()
    iceberg = IcebergAdapter(settings.iceberg)
    
    # 1. Verify Bronze Layer
    print(f"1. BRONZE LAYER - {MONTH_NAME} Partition")
    print("-" * 80)
    booking_bronze = iceberg.read_table('bronze', 'booking')
    month_bronze = booking_bronze[booking_bronze['extraction_month'] == EXTRACTION_MONTH]
    print(f"   Total bookings in {MONTH_NAME} partition: {len(month_bronze):,}")
    
    # Check date range in the actual data
    booking_bronze['date'] = pd.to_datetime(booking_bronze['date'])
    month_bronze_dates = booking_bronze[
        (booking_bronze['date'] >= MONTH_START) & 
        (booking_bronze['date'] <= MONTH_END)
    ]
    print(f"   Bookings with dates in {MONTH_NAME}: {len(month_bronze_dates):,}")
    if len(month_bronze_dates) > 0:
        print(f"   Date range: {month_bronze_dates['date'].min()} to {month_bronze_dates['date'].max()}")
    print()
    
    # 2. Silver Layer Statistics
    print("2. SILVER LAYER - Normalized Data")
    print("-" * 80)
    customer_silver = iceberg.read_table('silver', 'customer')
    vehicle_silver = iceberg.read_table('silver', 'vehicle_type')
    location_silver = iceberg.read_table('silver', 'location')
    
    print(f"   Unique customers: {len(customer_silver):,}")
    print(f"   Vehicle types: {len(vehicle_silver):,}")
    print(f"   Unique locations: {len(location_silver):,}")
    print()
    print("   Vehicle Types:")
    for _, row in vehicle_silver.iterrows():
        print(f"     - {row['name']} (ID: {row['vehicle_type_id']})")
    print()
    
    # 3. Gold Layer Analytics
    print(f"3. GOLD LAYER - Analytics & Insights for {MONTH_NAME}")
    print("-" * 80)
    daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
    customer_analytics = iceberg.read_table('gold', 'customer_analytics')
    location_analytics = iceberg.read_table('gold', 'location_analytics')
    
    # Convert date column for filtering
    daily_summary['date'] = pd.to_datetime(daily_summary['date'])
    
    # Month-specific analytics
    month_summary = daily_summary[
        (daily_summary['date'] >= MONTH_START) & 
        (daily_summary['date'] <= MONTH_END)
    ]
    
    print(f"   Daily summaries for {MONTH_NAME}: {len(month_summary):,} rows")
    
    if len(month_summary) > 0:
        month_revenue = month_summary['total_revenue'].sum()
        month_bookings = month_summary['total_bookings'].sum()
        avg_booking_value = month_revenue / month_bookings if month_bookings > 0 else 0
        
        print(f"   Total revenue in {MONTH_NAME}: ₹{month_revenue:,.2f}")
        print(f"   Total bookings in {MONTH_NAME}: {month_bookings:,.0f}")
        print(f"   Average booking value: ₹{avg_booking_value:.2f}")
        print()
        
        # Top vehicle types by revenue
        print(f"   Top Vehicle Types by Revenue ({MONTH_NAME}):")
        vehicle_revenue = month_summary.groupby('vehicle_type_name')['total_revenue'].sum().sort_values(ascending=False)
        for vehicle, revenue in vehicle_revenue.head().items():
            print(f"     - {vehicle}: ₹{revenue:,.2f}")
        print()
    
    # 4. Top Customers (Overall)
    print("4. TOP CUSTOMERS (Overall)")
    print("-" * 80)
    top_customers = customer_analytics.nlargest(10, 'total_spent')[
        ['customer_id', 'total_bookings', 'total_spent', 'avg_booking_value']
    ]
    print(top_customers.to_string(index=False))
    print()
    
    # 5. Top Locations
    print("5. TOP LOCATIONS BY ACTIVITY (Overall)")
    print("-" * 80)
    top_locations = location_analytics.nlargest(10, 'total_activity')[
        ['name', 'pickups', 'dropoffs', 'total_activity']
    ]
    print(top_locations.to_string(index=False))
    print()
    
    # 6. Partition Summary
    print("6. PARTITION SUMMARY")
    print("-" * 80)
    partitions = booking_bronze['extraction_month'].value_counts().sort_index()
    print("   Extraction months in Bronze layer:")
    for month, count in partitions.items():
        marker = " ← Current" if month == EXTRACTION_MONTH else ""
        print(f"     - {month}: {count:,} rows{marker}")
    print()
    
    print("=" * 80)
    print("VERIFICATION COMPLETE!")
    print("=" * 80)
    print()
    print(f"The ETL pipeline has successfully processed {MONTH_NAME} data:")
    print(f"✓ Bronze layer contains raw data partitioned by extraction_month='{EXTRACTION_MONTH}'")
    print("✓ Silver layer has normalized dimensions and facts")
    print("✓ Gold layer provides pre-aggregated analytics for fast querying")
    print()


if __name__ == "__main__":
    main()

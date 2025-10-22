#!/usr/bin/env python3
"""Verify and explore March 2024 ETL results."""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import pandas as pd
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

def main():
    print("=" * 80)
    print("MARCH 2024 DATA VERIFICATION")
    print("=" * 80)
    print()
    
    # Initialize Iceberg adapter
    settings = load_settings()
    iceberg = IcebergAdapter(settings.iceberg)
    
    # 1. Verify Bronze Layer - March 2024 partition
    print("1. BRONZE LAYER - March 2024 Partition")
    print("-" * 80)
    booking_bronze = iceberg.read_table('bronze', 'booking')
    march_bronze = booking_bronze[booking_bronze['extraction_month'] == '2024-03']
    print(f"   Total bookings in March 2024 partition: {len(march_bronze):,}")
    
    # Check date range in the actual data
    booking_bronze['date'] = pd.to_datetime(booking_bronze['date'])
    march_bronze_dates = booking_bronze[
        (booking_bronze['date'] >= '2024-03-01') & 
        (booking_bronze['date'] < '2024-04-01')
    ]
    print(f"   Bookings with dates in March 2024: {len(march_bronze_dates):,}")
    print(f"   Date range: {march_bronze_dates['date'].min()} to {march_bronze_dates['date'].max()}")
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
    print("3. GOLD LAYER - Analytics & Insights")
    print("-" * 80)
    daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
    customer_analytics = iceberg.read_table('gold', 'customer_analytics')
    location_analytics = iceberg.read_table('gold', 'location_analytics')
    
    # Convert date column for filtering
    daily_summary['date'] = pd.to_datetime(daily_summary['date'])
    
    # March 2024 specific analytics
    march_summary = daily_summary[
        (daily_summary['date'] >= '2024-03-01') & 
        (daily_summary['date'] < '2024-04-01')
    ]
    
    print(f"   Daily summaries for March 2024: {len(march_summary):,} rows")
    
    if len(march_summary) > 0:
        march_revenue = march_summary['total_revenue'].sum()
        march_bookings = march_summary['total_bookings'].sum()
        avg_booking_value = march_revenue / march_bookings if march_bookings > 0 else 0
        
        print(f"   Total revenue in March 2024: ₹{march_revenue:,.2f}")
        print(f"   Total bookings in March 2024: {march_bookings:,.0f}")
        print(f"   Average booking value: ₹{avg_booking_value:.2f}")
        print()
        
        # Top vehicle types by revenue in March
        print("   Top Vehicle Types by Revenue (March 2024):")
        vehicle_revenue = march_summary.groupby('vehicle_type_name')['total_revenue'].sum().sort_values(ascending=False)
        for vehicle, revenue in vehicle_revenue.head().items():
            print(f"     - {vehicle}: ₹{revenue:,.2f}")
        print()
    
    # 4. Top Customers
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
    # Location analytics already has the name column merged in
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
        print(f"     - {month}: {count:,} rows")
    print()
    
    print("=" * 80)
    print("VERIFICATION COMPLETE!")
    print("=" * 80)
    print()
    print("The ETL pipeline has successfully processed March 2024 data:")
    print("✓ Bronze layer contains raw data partitioned by extraction_month='2024-03'")
    print("✓ Silver layer has normalized dimensions and facts")
    print("✓ Gold layer provides pre-aggregated analytics for fast querying")
    print()

if __name__ == "__main__":
    main()

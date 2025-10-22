"""Gold layer: Aggregate Silver data into analytics-ready tables using Iceberg."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.adapters.iceberg_schemas import (
    GOLD_CUSTOMER_ANALYTICS_SCHEMA,
    GOLD_DAILY_BOOKING_SUMMARY_SCHEMA,
    GOLD_LOCATION_ANALYTICS_SCHEMA,
)

logger = logging.getLogger(__name__)


@task(name="aggregate-to-gold", retries=2, retry_delay_seconds=30)
def aggregate_to_gold(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Aggregate Silver layer data into Gold layer analytics tables.
    
    Args:
        iceberg_adapter: Iceberg adapter
        target_date: Specific date to process. If None, processes all data.
        
    Returns:
        Dictionary with row counts for each Gold table
    """
    logger.info(f"Starting Gold aggregation for date: {target_date or 'all'}")
    
    row_counts = {}
    
    # Create aggregated analytics
    row_counts['gold.daily_booking_summary'] = _aggregate_daily_summary(iceberg_adapter, target_date)
    row_counts['gold.customer_analytics'] = _aggregate_customer_analytics(iceberg_adapter)
    row_counts['gold.location_analytics'] = _aggregate_location_analytics(iceberg_adapter)
    
    logger.info(f"Gold aggregation completed. Row counts: {row_counts}")
    return row_counts


def _aggregate_daily_summary(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime]
) -> int:
    """Aggregate daily booking summary statistics."""
    
    # Load Silver tables
    bookings = iceberg_adapter.read_table('silver', 'booking')
    rides = iceberg_adapter.read_table('silver', 'ride')
    cancelled_rides = iceberg_adapter.read_table('silver', 'cancelled_ride')
    incompleted_rides = iceberg_adapter.read_table('silver', 'incompleted_ride')
    
    if len(bookings) == 0:
        logger.info("No data to aggregate for daily summary")
        return 0
    
    # Filter by date if specified
    if target_date:
        bookings = bookings[bookings['date'] == target_date.date()]
    
    # Join with fact tables
    bookings = bookings.merge(rides[['booking_id', 'ride_distance', 'driver_rating', 'customer_rating']], 
                              on='booking_id', how='left')
    bookings = bookings.merge(cancelled_rides[['booking_id', 'cancellation_id']], 
                              on='booking_id', how='left')
    bookings = bookings.merge(incompleted_rides[['booking_id', 'incompleted_id']], 
                              on='booking_id', how='left')
    
    # Aggregate by date
    summary_df = bookings.groupby('date').agg({
        'booking_id': 'nunique',
        'ride_distance': 'mean',
        'driver_rating': 'mean',
        'customer_rating': 'mean',
        'booking_value': 'sum',
    }).reset_index()
    
    # Count completed, cancelled, and incompleted rides
    completed_counts = bookings[bookings['ride_distance'].notna()].groupby('date').size()
    cancelled_counts = bookings[bookings['cancellation_id'].notna()].groupby('date').size()
    incompleted_counts = bookings[bookings['incompleted_id'].notna()].groupby('date').size()
    
    summary_df = summary_df.merge(completed_counts.to_frame('completed_rides'), 
                                  left_on='date', right_index=True, how='left')
    summary_df = summary_df.merge(cancelled_counts.to_frame('cancelled_rides'), 
                                  left_on='date', right_index=True, how='left')
    summary_df = summary_df.merge(incompleted_counts.to_frame('incompleted_rides'), 
                                  left_on='date', right_index=True, how='left')
    
    # Fill NaN values
    summary_df = summary_df.fillna(0)
    
    # Rename columns
    summary_df.columns = [
        'summary_date', 'total_bookings', 'avg_ride_distance', 'avg_driver_rating',
        'avg_customer_rating', 'total_revenue', 'completed_rides', 'cancelled_rides',
        'incompleted_rides'
    ]
    
    # Convert counts to int
    for col in ['total_bookings', 'completed_rides', 'cancelled_rides', 'incompleted_rides']:
        summary_df[col] = summary_df[col].astype(int)
    
    summary_df['created_at'] = datetime.now()
    summary_df['updated_at'] = datetime.now()
    
    # Reorder columns to match schema
    summary_df = summary_df[[
        'summary_date', 'total_bookings', 'completed_rides', 'cancelled_rides',
        'incompleted_rides', 'total_revenue', 'avg_ride_distance',
        'avg_driver_rating', 'avg_customer_rating', 'created_at', 'updated_at'
    ]]
    
    if not iceberg_adapter.table_exists('gold', 'daily_booking_summary'):
        iceberg_adapter.create_table('gold', 'daily_booking_summary', GOLD_DAILY_BOOKING_SUMMARY_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(summary_df, 'gold', 'daily_booking_summary', mode='overwrite')
    logger.info(f"Aggregated {rows_written} daily summaries to Gold")
    return rows_written


def _aggregate_customer_analytics(iceberg_adapter: IcebergAdapter) -> int:
    """Aggregate customer-level analytics."""
    
    # Load Silver tables
    customers = iceberg_adapter.read_table('silver', 'customer')
    bookings = iceberg_adapter.read_table('silver', 'booking')
    rides = iceberg_adapter.read_table('silver', 'ride')
    cancelled_rides = iceberg_adapter.read_table('silver', 'cancelled_ride')
    vehicle_types = iceberg_adapter.read_table('silver', 'vehicle_type')
    
    if len(customers) == 0:
        logger.info("No data to aggregate for customer analytics")
        return 0
    
    # Join bookings with rides and cancelled rides
    bookings_with_facts = bookings.merge(rides[['booking_id', 'customer_rating']], 
                                         on='booking_id', how='left')
    bookings_with_facts = bookings_with_facts.merge(cancelled_rides[['booking_id', 'cancellation_id']], 
                                                     on='booking_id', how='left')
    bookings_with_facts = bookings_with_facts.merge(vehicle_types[['vehicle_type_id', 'name']], 
                                                     on='vehicle_type_id', how='left')
    
    # Aggregate by customer
    customer_analytics = bookings_with_facts.groupby('customer_id').agg({
        'booking_id': 'nunique',
        'booking_value': 'sum',
        'customer_rating': 'mean',
        'date': ['min', 'max'],
        'name': lambda x: x.mode()[0] if len(x.mode()) > 0 else None
    }).reset_index()
    
    # Flatten multi-level columns
    customer_analytics.columns = ['customer_id', 'total_bookings', 'total_spent', 
                                   'avg_rating', 'first_booking_date', 'last_booking_date',
                                   'favorite_vehicle_type']
    
    # Count completed and cancelled rides
    completed_counts = bookings_with_facts[bookings_with_facts['customer_rating'].notna()].groupby('customer_id').size()
    cancelled_counts = bookings_with_facts[bookings_with_facts['cancellation_id'].notna()].groupby('customer_id').size()
    
    customer_analytics = customer_analytics.merge(completed_counts.to_frame('completed_rides'), 
                                                   left_on='customer_id', right_index=True, how='left')
    customer_analytics = customer_analytics.merge(cancelled_counts.to_frame('cancelled_rides'), 
                                                   left_on='customer_id', right_index=True, how='left')
    
    customer_analytics = customer_analytics.fillna(0)
    
    # Convert to int where appropriate
    for col in ['total_bookings', 'completed_rides', 'cancelled_rides']:
        customer_analytics[col] = customer_analytics[col].astype(int)
    
    customer_analytics['created_at'] = datetime.now()
    customer_analytics['updated_at'] = datetime.now()
    
    # Reorder columns
    customer_analytics = customer_analytics[[
        'customer_id', 'total_bookings', 'completed_rides', 'cancelled_rides',
        'total_spent', 'avg_rating', 'favorite_vehicle_type',
        'first_booking_date', 'last_booking_date', 'created_at', 'updated_at'
    ]]
    
    if not iceberg_adapter.table_exists('gold', 'customer_analytics'):
        iceberg_adapter.create_table('gold', 'customer_analytics', GOLD_CUSTOMER_ANALYTICS_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(customer_analytics, 'gold', 'customer_analytics', mode='overwrite')
    logger.info(f"Aggregated {rows_written} customer analytics to Gold")
    return rows_written


def _aggregate_location_analytics(iceberg_adapter: IcebergAdapter) -> int:
    """Aggregate location-level analytics."""
    
    # Load Silver tables
    locations = iceberg_adapter.read_table('silver', 'location')
    bookings = iceberg_adapter.read_table('silver', 'booking')
    
    if len(locations) == 0:
        logger.info("No data to aggregate for location analytics")
        return 0
    
    # Count pickups
    pickup_counts = bookings.groupby('pickup_location_id').agg({
        'booking_id': 'nunique',
        'booking_value': 'mean'
    }).reset_index()
    pickup_counts.columns = ['location_id', 'total_pickups', 'avg_booking_value']
    
    # Count drops
    drop_counts = bookings.groupby('drop_location_id').size().reset_index(name='total_drops')
    drop_counts.columns = ['location_id', 'total_drops']
    
    # Merge with locations
    location_analytics = locations.merge(pickup_counts, on='location_id', how='left')
    location_analytics = location_analytics.merge(drop_counts, on='location_id', how='left')
    
    location_analytics = location_analytics.fillna(0)
    
    # Convert to int where appropriate
    for col in ['total_pickups', 'total_drops']:
        location_analytics[col] = location_analytics[col].astype(int)
    
    # Rename and select columns
    location_analytics = location_analytics.rename(columns={'name': 'location_name'})
    location_analytics['created_at'] = datetime.now()
    location_analytics['updated_at'] = datetime.now()
    
    location_analytics = location_analytics[[
        'location_id', 'location_name', 'total_pickups', 'total_drops',
        'avg_booking_value', 'created_at', 'updated_at'
    ]]
    
    if not iceberg_adapter.table_exists('gold', 'location_analytics'):
        iceberg_adapter.create_table('gold', 'location_analytics', GOLD_LOCATION_ANALYTICS_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(location_analytics, 'gold', 'location_analytics', mode='overwrite')
    logger.info(f"Aggregated {rows_written} location analytics to Gold")
    return rows_written

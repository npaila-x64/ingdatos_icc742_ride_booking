"""Gold layer: Aggregate Silver data into analytics-ready tables using Iceberg."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.adapters import iceberg_schemas

logger = logging.getLogger(__name__)


def _initialize_gold_tables(iceberg_adapter: IcebergAdapter) -> None:
    """Initialize all Gold layer tables if they don't exist."""
    tables = {
        'daily_booking_summary': iceberg_schemas.GOLD_DAILY_BOOKING_SUMMARY_SCHEMA,
        'customer_analytics': iceberg_schemas.GOLD_CUSTOMER_ANALYTICS_SCHEMA,
        'location_analytics': iceberg_schemas.GOLD_LOCATION_ANALYTICS_SCHEMA,
    }
    
    for table_name, schema in tables.items():
        table = iceberg_adapter.get_table('gold', table_name)
        if table is None:
            logger.info(f"Creating table gold.{table_name}")
            iceberg_adapter.create_table('gold', table_name, schema)
        else:
            logger.info(f"Table gold.{table_name} already exists")


@task(name="aggregate-to-gold", retries=2, retry_delay_seconds=30)
def aggregate_to_gold(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Aggregate Silver layer data into Gold layer analytics tables."""
    logger.info(f"Starting Gold aggregation for date: {target_date or 'all'}")
    
    # Initialize Gold tables if they don't exist
    _initialize_gold_tables(iceberg_adapter)
    
    row_counts = {}
    
    # Create aggregated analytics
    row_counts['gold.daily_booking_summary'] = _aggregate_daily_summary(iceberg_adapter, target_date)
    row_counts['gold.customer_analytics'] = _aggregate_customer_analytics(iceberg_adapter)
    row_counts['gold.location_analytics'] = _aggregate_location_analytics(iceberg_adapter)
    
    logger.info(f"Gold aggregation completed. Row counts: {row_counts}")
    return row_counts


def _aggregate_daily_summary(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime] = None,
) -> int:
    """Create daily booking summary from Silver layer."""
    logger.info("Aggregating daily booking summary")
    
    # Read silver booking data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    if bookings is None or len(bookings) == 0:
        logger.warning("No bookings in Silver layer")
        return 0
    
    # Read dimension tables for names
    vehicle_types = iceberg_adapter.read_table('silver', 'vehicle_type')
    booking_statuses = iceberg_adapter.read_table('silver', 'booking_status')
    
    if vehicle_types is None or booking_statuses is None:
        logger.warning("Missing dimension tables")
        return 0
    
    # Merge dimension names
    bookings = bookings.merge(
        vehicle_types[['vehicle_type_id', 'name']], 
        on='vehicle_type_id', 
        how='left'
    ).rename(columns={'name': 'vehicle_type_name'})
    
    bookings = bookings.merge(
        booking_statuses[['booking_status_id', 'name']], 
        on='booking_status_id', 
        how='left'
    ).rename(columns={'name': 'booking_status_name'})
    
    # Aggregate by date, vehicle type, and status
    daily_summary = bookings.groupby([
        'date', 'vehicle_type_name', 'booking_status_name'
    ]).agg(
        total_bookings=('booking_id', 'count'),
        total_revenue=('booking_value', 'sum'),
        avg_booking_value=('booking_value', 'mean')
    ).reset_index()
    
    # Add timestamp
    now = datetime.utcnow()
    daily_summary['created_at'] = now
    daily_summary['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        daily_summary, 'gold', 'daily_booking_summary', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} daily summaries")
    return rows_written


def _aggregate_customer_analytics(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Create customer analytics from Silver layer."""
    logger.info("Aggregating customer analytics")
    
    # Read silver data
    customers = iceberg_adapter.read_table('silver', 'customer')
    bookings = iceberg_adapter.read_table('silver', 'booking')
    
    if customers is None or bookings is None:
        logger.warning("Missing data in Silver layer")
        return 0
    
    # Calculate customer metrics
    customer_stats = bookings.groupby('customer_id').agg(
        total_bookings=('booking_id', 'count'),
        total_spent=('booking_value', 'sum'),
        avg_booking_value=('booking_value', 'mean'),
        first_booking_date=('date', 'min'),
        last_booking_date=('date', 'max')
    ).reset_index()
    
    # Merge with customer data (drop total_bookings from customers to avoid conflict)
    customers_clean = customers.drop(columns=['total_bookings'], errors='ignore')
    customer_analytics = customers_clean.merge(customer_stats, on='customer_id', how='left')
    
    # Calculate customer lifetime days (convert dates to datetime first)
    customer_analytics['customer_lifetime_days'] = (
        pd.to_datetime(customer_analytics['last_booking_date']) - 
        pd.to_datetime(customer_analytics['first_booking_date'])
    ).dt.days
    
    # Add timestamp
    now = datetime.utcnow()
    customer_analytics['created_at'] = now
    customer_analytics['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        customer_analytics, 'gold', 'customer_analytics', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} customer analytics")
    return rows_written


def _aggregate_location_analytics(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Create location analytics from Silver layer."""
    logger.info("Aggregating location analytics")
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    locations = iceberg_adapter.read_table('silver', 'location')
    
    if bookings is None or locations is None:
        logger.warning("Missing data in Silver layer")
        return 0
    
    # Calculate pickup location metrics
    pickup_stats = bookings.groupby('pickup_location_id').agg(
        pickups=('booking_id', 'count'),
        avg_booking_value=('booking_value', 'mean')
    ).reset_index()
    pickup_stats.rename(columns={'pickup_location_id': 'location_id'}, inplace=True)
    
    # Calculate dropoff location metrics
    dropoff_stats = bookings.groupby('drop_location_id').agg(
        dropoffs=('booking_id', 'count')
    ).reset_index()
    dropoff_stats.rename(columns={'drop_location_id': 'location_id'}, inplace=True)
    
    # Merge location analytics
    location_analytics = locations.merge(pickup_stats, on='location_id', how='left')
    location_analytics = location_analytics.merge(dropoff_stats, on='location_id', how='left')
    
    # Fill NaN values with 0
    location_analytics['pickups'] = location_analytics['pickups'].fillna(0).astype(int)
    location_analytics['dropoffs'] = location_analytics['dropoffs'].fillna(0).astype(int)
    location_analytics['avg_booking_value'] = location_analytics['avg_booking_value'].fillna(0)
    
    # Calculate total activity
    location_analytics['total_activity'] = location_analytics['pickups'] + location_analytics['dropoffs']
    
    # Add timestamp
    now = datetime.utcnow()
    location_analytics['created_at'] = now
    location_analytics['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        location_analytics, 'gold', 'location_analytics', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} location analytics")
    return rows_written

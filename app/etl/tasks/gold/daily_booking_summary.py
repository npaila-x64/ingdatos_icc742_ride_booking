"""Gold layer: Aggregate daily booking summary analytics."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import pandas as pd
from prefect import task

from app.adapters import iceberg_schemas

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="aggregate-gold-daily-booking-summary", retries=2, retry_delay_seconds=30)
def aggregate_gold_daily_booking_summary(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime] = None,
) -> int:
    """Create daily booking summary aggregation from Silver layer.
    
    Aggregates bookings by date, vehicle type, and booking status with metrics:
    - total_bookings
    - total_revenue
    - avg_booking_value
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        target_date: Optional target date for filtering
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating daily booking summary")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'daily_booking_summary'):
        logger.info("Creating table gold.daily_booking_summary")
        iceberg_adapter.create_table('gold', 'daily_booking_summary', iceberg_schemas.GOLD_DAILY_BOOKING_SUMMARY_SCHEMA)
    
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
    
    logger.info(f"Aggregated {rows_written} daily booking summary rows to Gold")
    return rows_written

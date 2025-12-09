"""Gold layer: Average revenue per completed ride KPI."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters import iceberg_schemas

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="aggregate-gold-avg-revenue-per-ride", retries=2, retry_delay_seconds=30)
def aggregate_gold_avg_revenue_per_ride(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate average revenue per completed ride KPI.
    
    Business question: ¿Cuánto ingreso generamos en promedio por cada viaje completado?
    
    Calculation: Total revenue / Completed rides
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating average revenue per ride KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'avg_revenue_per_ride'):
        logger.info("Creating table gold.avg_revenue_per_ride")
        iceberg_adapter.create_table('gold', 'avg_revenue_per_ride', iceberg_schemas.GOLD_AVG_REVENUE_PER_RIDE_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    rides = iceberg_adapter.read_table('silver', 'ride')
    booking_status = iceberg_adapter.read_table('silver', 'booking_status')
    
    if bookings is None or len(bookings) == 0:
        logger.warning("No booking data in Silver layer")
        return 0
    
    # Get extraction_month from bronze.booking
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    if bronze_booking is not None and 'extraction_month' in bronze_booking.columns:
        # Merge extraction_month into silver bookings
        extraction_mapping = bronze_booking[['booking_id', 'extraction_month']].drop_duplicates()
        bookings = bookings.merge(extraction_mapping, on='booking_id', how='left')
    
    # Filter for completed rides (those that have ride data or status "Success")
    if rides is not None and len(rides) > 0:
        # Get completed bookings (those with ride records)
        completed_bookings = bookings.merge(
            rides[['booking_id']].drop_duplicates(),
            on='booking_id',
            how='inner'
        )
    elif booking_status is not None and len(booking_status) > 0:
        # Try to use booking status
        success_status = booking_status[booking_status['name'].str.lower() == 'success']
        if len(success_status) > 0:
            completed_bookings = bookings.merge(
                success_status[['booking_status_id']],
                on='booking_status_id',
                how='inner'
            )
        else:
            logger.warning("No 'Success' status found, using all bookings")
            completed_bookings = bookings.copy()
    else:
        logger.warning("No ride or status data available, using all bookings")
        completed_bookings = bookings.copy()
    
    # Calculate metrics by date
    groupby_cols = ['date']
    include_extraction_month = 'extraction_month' in completed_bookings.columns
    if include_extraction_month:
        groupby_cols.append('extraction_month')
    
    revenue_per_ride = completed_bookings.groupby(groupby_cols).agg(
        completed_rides=('booking_id', 'count'),
        total_revenue=('booking_value', 'sum')
    ).reset_index()
    
    # Calculate average revenue per ride
    revenue_per_ride['avg_revenue_per_ride'] = (
        revenue_per_ride['total_revenue'] / revenue_per_ride['completed_rides']
    ).fillna(0)
    
    # Add timestamps
    now = datetime.utcnow()
    revenue_per_ride['created_at'] = now
    revenue_per_ride['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        revenue_per_ride, 'gold', 'avg_revenue_per_ride', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} average revenue per ride rows to Gold")
    return rows_written

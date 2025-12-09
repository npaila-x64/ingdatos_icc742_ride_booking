"""Gold layer: Cancellation rate KPI."""

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


@task(name="aggregate-gold-cancellation-rate", retries=2, retry_delay_seconds=30)
def aggregate_gold_cancellation_rate(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate daily cancellation rate KPI.
    
    Business question: ¿Cuál es el porcentaje de viajes cancelados por día?
    
    Calculation: (Cancelled bookings / Total bookings) × 100
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating cancellation rate KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'cancellation_rate'):
        logger.info("Creating table gold.cancellation_rate")
        iceberg_adapter.create_table('gold', 'cancellation_rate', iceberg_schemas.GOLD_CANCELLATION_RATE_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    cancelled_rides = iceberg_adapter.read_table('silver', 'cancelled_ride')
    
    if bookings is None or len(bookings) == 0:
        logger.warning("No booking data in Silver layer")
        return 0
    
    # Get extraction_month from bronze.booking
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    if bronze_booking is not None and 'extraction_month' in bronze_booking.columns:
        # Merge extraction_month into silver bookings
        extraction_mapping = bronze_booking[['booking_id', 'extraction_month']].drop_duplicates()
        bookings = bookings.merge(extraction_mapping, on='booking_id', how='left')
    
    # Get all bookings by date
    groupby_cols = ['date']
    include_extraction_month = 'extraction_month' in bookings.columns
    if include_extraction_month:
        groupby_cols.append('extraction_month')
    
    total_bookings_by_date = bookings.groupby(groupby_cols).agg(
        total_bookings=('booking_id', 'count')
    ).reset_index()
    
    # Get cancelled bookings
    if cancelled_rides is not None and len(cancelled_rides) > 0:
        # Join with bookings to get date
        merge_cols = ['booking_id', 'date']
        if include_extraction_month:
            merge_cols.append('extraction_month')
        
        cancelled_with_date = cancelled_rides.merge(
            bookings[merge_cols],
            on='booking_id',
            how='inner'
        )
        
        cancelled_by_date = cancelled_with_date.groupby(groupby_cols).agg(
            cancelled_bookings=('booking_id', 'count')
        ).reset_index()
        
        # Merge with total bookings
        cancellation_rate = total_bookings_by_date.merge(
            cancelled_by_date,
            on=groupby_cols,
            how='left'
        )
    else:
        cancellation_rate = total_bookings_by_date.copy()
        cancellation_rate['cancelled_bookings'] = 0
    
    # Fill NaN with 0 for dates with no cancellations
    cancellation_rate['cancelled_bookings'] = cancellation_rate['cancelled_bookings'].fillna(0).astype(int)
    
    # Calculate cancellation rate percentage
    cancellation_rate['cancellation_rate_pct'] = (
        cancellation_rate['cancelled_bookings'] / cancellation_rate['total_bookings'] * 100
    ).fillna(0)
    
    # Add timestamps
    now = datetime.utcnow()
    cancellation_rate['created_at'] = now
    cancellation_rate['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        cancellation_rate, 'gold', 'cancellation_rate', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} cancellation rate rows to Gold")
    return rows_written

"""Gold layer: Incomplete ride rate KPI."""

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


@task(name="aggregate-gold-incomplete-rate", retries=2, retry_delay_seconds=30)
def aggregate_gold_incomplete_rate(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate daily incomplete ride rate KPI.
    
    Business question: ¿Cuál es el porcentaje de viajes que quedaron incompletos?
    
    Calculation: (Incomplete rides / Total bookings) × 100
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating incomplete rate KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'incomplete_rate'):
        logger.info("Creating table gold.incomplete_rate")
        iceberg_adapter.create_table('gold', 'incomplete_rate', iceberg_schemas.GOLD_INCOMPLETE_RATE_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    incompleted_rides = iceberg_adapter.read_table('silver', 'incompleted_ride')
    
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
    
    # Get incomplete bookings
    if incompleted_rides is not None and len(incompleted_rides) > 0:
        # Join with bookings to get date
        merge_cols = ['booking_id', 'date']
        if include_extraction_month:
            merge_cols.append('extraction_month')
        
        incomplete_with_date = incompleted_rides.merge(
            bookings[merge_cols],
            on='booking_id',
            how='inner'
        )
        
        incomplete_by_date = incomplete_with_date.groupby(groupby_cols).agg(
            incomplete_bookings=('booking_id', 'count')
        ).reset_index()
        
        # Merge with total bookings
        incomplete_rate = total_bookings_by_date.merge(
            incomplete_by_date,
            on=groupby_cols,
            how='left'
        )
    else:
        incomplete_rate = total_bookings_by_date.copy()
        incomplete_rate['incomplete_bookings'] = 0
    
    # Fill NaN with 0 for dates with no incomplete rides
    incomplete_rate['incomplete_bookings'] = incomplete_rate['incomplete_bookings'].fillna(0).astype(int)
    
    # Calculate incomplete rate percentage
    incomplete_rate['incomplete_rate_pct'] = (
        incomplete_rate['incomplete_bookings'] / incomplete_rate['total_bookings'] * 100
    ).fillna(0)
    
    # Add timestamps
    now = datetime.utcnow()
    incomplete_rate['created_at'] = now
    incomplete_rate['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        incomplete_rate, 'gold', 'incomplete_rate', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} incomplete rate rows to Gold")
    return rows_written

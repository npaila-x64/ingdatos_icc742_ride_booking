"""Bronze layer: Extract booking (fact) data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_BOOKING_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-booking", retries=2, retry_delay_seconds=30)
def extract_bronze_booking(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract booking fact data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting booking data to Bronze")
    
    booking_df = source_df[[
        'Booking ID', 'Customer ID', 'Vehicle Type', 'Pickup Location', 'Drop Location',
        'Booking Status', 'Payment Method', 'Booking Value', 'Date', 'Time',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    booking_df.columns = [
        'booking_id', 'customer_id', 'vehicle_type', 'pickup_location', 'drop_location',
        'booking_status', 'payment_method', 'booking_value', 'date', 'time',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    booking_df = booking_df.dropna(subset=['booking_id'])
    booking_df = booking_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'booking'):
        logger.info("Creating bronze.booking table")
        iceberg_adapter.create_table('bronze', 'booking', BRONZE_BOOKING_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(booking_df, 'bronze', 'booking', mode='append')
    logger.info(f"Wrote {rows_written} booking rows to Bronze")
    
    return rows_written

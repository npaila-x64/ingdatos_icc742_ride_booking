"""Bronze layer: Extract booking status data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_BOOKING_STATUS_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-booking-status", retries=2, retry_delay_seconds=30)
def extract_bronze_booking_status(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract booking status data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting booking status data to Bronze")
    
    status_df = source_df[
        ['Booking Status', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    
    status_df.columns = ['booking_status_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    status_df = status_df.dropna(subset=['booking_status_name'])
    status_df = status_df.drop_duplicates(subset=['booking_status_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'booking_status'):
        logger.info("Creating bronze.booking_status table")
        iceberg_adapter.create_table('bronze', 'booking_status', BRONZE_BOOKING_STATUS_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(status_df, 'bronze', 'booking_status', mode='append')
    logger.info(f"Wrote {rows_written} booking status rows to Bronze")
    
    return rows_written

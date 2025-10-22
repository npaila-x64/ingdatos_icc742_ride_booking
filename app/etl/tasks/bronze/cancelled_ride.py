"""Bronze layer: Extract cancelled ride data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_CANCELLED_RIDE_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-cancelled-ride", retries=2, retry_delay_seconds=30)
def extract_bronze_cancelled_ride(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract cancelled ride data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting cancelled ride data to Bronze")
    
    cancelled_df = source_df[
        (source_df['Cancelled Rides by Customer'].notna()) | (source_df['Cancelled Rides by Driver'].notna())
    ][[
        'Booking ID', 'Cancelled Rides by Customer', 'Cancelled Rides by Driver',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    cancelled_df.columns = [
        'booking_id', 'cancelled_rides_by_customer', 'cancelled_rides_by_driver',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    cancelled_df = cancelled_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if len(cancelled_df) == 0:
        logger.info("No cancelled rides in source data")
        return 0
    
    # Convert to int (handling NaN)
    cancelled_df['cancelled_rides_by_customer'] = cancelled_df['cancelled_rides_by_customer'].fillna(0).astype(int)
    cancelled_df['cancelled_rides_by_driver'] = cancelled_df['cancelled_rides_by_driver'].fillna(0).astype(int)
    
    if not iceberg_adapter.table_exists('bronze', 'cancelled_ride'):
        logger.info("Creating bronze.cancelled_ride table")
        iceberg_adapter.create_table('bronze', 'cancelled_ride', BRONZE_CANCELLED_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(cancelled_df, 'bronze', 'cancelled_ride', mode='append')
    logger.info(f"Wrote {rows_written} cancelled ride rows to Bronze")
    
    return rows_written

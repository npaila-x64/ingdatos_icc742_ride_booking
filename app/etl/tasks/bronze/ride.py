"""Bronze layer: Extract ride (completed) data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_RIDE_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-ride", retries=2, retry_delay_seconds=30)
def extract_bronze_ride(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract completed ride data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting ride data to Bronze")
    
    ride_df = source_df[source_df['Booking Status'] == 'Completed'][[
        'Booking ID', 'Ride Distance', 'Driver Ratings', 'Customer Rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    ride_df.columns = [
        'booking_id', 'ride_distance', 'driver_rating', 'customer_rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    ride_df = ride_df.dropna(subset=['booking_id'])
    ride_df = ride_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'ride'):
        logger.info("Creating bronze.ride table")
        iceberg_adapter.create_table('bronze', 'ride', BRONZE_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(ride_df, 'bronze', 'ride', mode='append')
    logger.info(f"Wrote {rows_written} ride rows to Bronze")
    
    return rows_written

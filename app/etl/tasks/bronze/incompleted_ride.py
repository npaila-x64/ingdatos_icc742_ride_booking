"""Bronze layer: Extract incompleted ride data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_INCOMPLETED_RIDE_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-incompleted-ride", retries=2, retry_delay_seconds=30)
def extract_bronze_incompleted_ride(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract incompleted ride data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting incompleted ride data to Bronze")
    
    incompleted_df = source_df[source_df['Incomplete Rides'].notna()][[
        'Booking ID', 'Incomplete Rides', 'Incomplete Rides Reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    incompleted_df.columns = [
        'booking_id', 'incomplete_rides', 'incomplete_rides_reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    incompleted_df = incompleted_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if len(incompleted_df) == 0:
        logger.info("No incompleted rides in source data")
        return 0
    
    # Convert to int
    incompleted_df['incomplete_rides'] = incompleted_df['incomplete_rides'].fillna(0).astype(int)
    
    if not iceberg_adapter.table_exists('bronze', 'incompleted_ride'):
        logger.info("Creating bronze.incompleted_ride table")
        iceberg_adapter.create_table('bronze', 'incompleted_ride', BRONZE_INCOMPLETED_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(incompleted_df, 'bronze', 'incompleted_ride', mode='append')
    logger.info(f"Wrote {rows_written} incompleted ride rows to Bronze")
    
    return rows_written

"""Bronze layer: Extract location data (pickup and drop) from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_LOCATION_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-location", retries=2, retry_delay_seconds=30)
def extract_bronze_location(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract location data (pickup and drop) to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting location data to Bronze")
    
    # Pickup locations
    pickup_df = source_df[
        ['Pickup Location', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    pickup_df.columns = ['location_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    pickup_df['location_type'] = 'pickup'
    pickup_df = pickup_df.dropna(subset=['location_name'])
    
    # Drop locations
    drop_df = source_df[
        ['Drop Location', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    drop_df.columns = ['location_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    drop_df['location_type'] = 'drop'
    drop_df = drop_df.dropna(subset=['location_name'])
    
    # Combine and deduplicate
    location_df = pd.concat([pickup_df, drop_df], ignore_index=True)
    location_df = location_df[
        ['location_name', 'location_type', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    ]
    location_df = location_df.drop_duplicates(
        subset=['location_name', 'location_type', 'booking_id', 'extraction_month']
    )
    
    if not iceberg_adapter.table_exists('bronze', 'location'):
        logger.info("Creating bronze.location table")
        iceberg_adapter.create_table('bronze', 'location', BRONZE_LOCATION_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(location_df, 'bronze', 'location', mode='append')
    logger.info(f"Wrote {rows_written} location rows to Bronze")
    
    return rows_written

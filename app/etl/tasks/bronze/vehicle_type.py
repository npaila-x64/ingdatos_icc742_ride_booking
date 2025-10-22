"""Bronze layer: Extract vehicle type data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_VEHICLE_TYPE_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-vehicle-type", retries=2, retry_delay_seconds=30)
def extract_bronze_vehicle_type(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract vehicle type data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting vehicle type data to Bronze")
    
    vehicle_df = source_df[
        ['Vehicle Type', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    
    vehicle_df.columns = ['vehicle_type_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    vehicle_df = vehicle_df.dropna(subset=['vehicle_type_name'])
    vehicle_df = vehicle_df.drop_duplicates(subset=['vehicle_type_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'vehicle_type'):
        logger.info("Creating bronze.vehicle_type table")
        iceberg_adapter.create_table('bronze', 'vehicle_type', BRONZE_VEHICLE_TYPE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(vehicle_df, 'bronze', 'vehicle_type', mode='append')
    logger.info(f"Wrote {rows_written} vehicle type rows to Bronze")
    
    return rows_written

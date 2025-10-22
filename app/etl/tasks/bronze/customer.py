"""Bronze layer: Extract customer data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_CUSTOMER_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-customer", retries=2, retry_delay_seconds=30)
def extract_bronze_customer(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract customer data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting customer data to Bronze")
    
    customer_df = source_df[
        ['Customer ID', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    
    customer_df.columns = ['customer_id', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    customer_df = customer_df.dropna(subset=['customer_id'])
    customer_df = customer_df.drop_duplicates(subset=['customer_id', 'booking_id', 'extraction_month'])
    
    # Ensure table exists
    if not iceberg_adapter.table_exists('bronze', 'customer'):
        logger.info("Creating bronze.customer table")
        iceberg_adapter.create_table('bronze', 'customer', BRONZE_CUSTOMER_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(customer_df, 'bronze', 'customer', mode='append')
    logger.info(f"Wrote {rows_written} customer rows to Bronze")
    
    return rows_written

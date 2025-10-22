"""Bronze layer: Extract payment method data from source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from prefect import task

from app.adapters.iceberg_schemas import BRONZE_PAYMENT_METHOD_SCHEMA

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="extract-bronze-payment-method", retries=2, retry_delay_seconds=30)
def extract_bronze_payment_method(
    source_df: pd.DataFrame,
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Extract payment method data to Bronze layer.
    
    Args:
        source_df: Source DataFrame with metadata columns already added
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Extracting payment method data to Bronze")
    
    payment_df = source_df[
        ['Payment Method', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']
    ].copy()
    
    payment_df.columns = ['payment_method_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    payment_df = payment_df.dropna(subset=['payment_method_name'])
    payment_df = payment_df.drop_duplicates(subset=['payment_method_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'payment_method'):
        logger.info("Creating bronze.payment_method table")
        iceberg_adapter.create_table('bronze', 'payment_method', BRONZE_PAYMENT_METHOD_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(payment_df, 'bronze', 'payment_method', mode='append')
    logger.info(f"Wrote {rows_written} payment method rows to Bronze")
    
    return rows_written

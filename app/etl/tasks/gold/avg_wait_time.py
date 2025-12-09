"""Gold layer: Average vehicle arrival time KPI."""

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


@task(name="aggregate-gold-avg-wait-time", retries=2, retry_delay_seconds=30)
def aggregate_gold_avg_wait_time(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate average vehicle arrival time KPI.
    
    Business question: ¿Cuánto tiempo esperan los usuarios hasta que llega el vehículo?
    
    Uses: Avg VTAT (Vehicle Turn-Around Time) from Bronze data
    
    Calculation: Average of VTAT across completed rides
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating average vehicle arrival time KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'avg_wait_time'):
        logger.info("Creating table gold.avg_wait_time")
        iceberg_adapter.create_table('gold', 'avg_wait_time', iceberg_schemas.GOLD_AVG_WAIT_TIME_SCHEMA)
    
    # Read bronze data (has VTAT column)
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    
    if bronze_booking is None or len(bronze_booking) == 0:
        logger.warning("No booking data in Bronze layer")
        return 0
    
    # Parse the avg_vtat column
    # Convert to numeric, handling null values
    if 'avg_vtat' in bronze_booking.columns:
        bronze_booking['avg_vtat_numeric'] = pd.to_numeric(
            bronze_booking['avg_vtat'], 
            errors='coerce'
        )
        
        # Filter only rows with valid VTAT (completed rides)
        valid_vtat = bronze_booking[bronze_booking['avg_vtat_numeric'].notna()].copy()
        
        if len(valid_vtat) == 0:
            logger.warning("No valid VTAT data found")
            return 0
        
        # Calculate metrics by date
        groupby_cols = ['date']
        include_extraction_month = 'extraction_month' in valid_vtat.columns
        if include_extraction_month:
            groupby_cols.append('extraction_month')
        
        wait_time = valid_vtat.groupby(groupby_cols).agg(
            avg_wait_time_minutes=('avg_vtat_numeric', 'mean'),
            total_bookings=('booking_id', 'count')
        ).reset_index()
        
        logger.info(f"Calculated VTAT for {len(wait_time)} date groups")
    else:
        logger.error("Column 'avg_vtat' not found in bronze.booking")
        return 0
    
    # Add timestamps
    now = datetime.utcnow()
    wait_time['created_at'] = now
    wait_time['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        wait_time, 'gold', 'avg_wait_time', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} average vehicle arrival time rows to Gold")
    return rows_written

"""Gold layer: Aggregate customer analytics."""

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


@task(name="aggregate-gold-customer-analytics", retries=2, retry_delay_seconds=30)
def aggregate_gold_customer_analytics(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Create customer analytics aggregation from Silver layer.
    
    Aggregates customer-level metrics:
    - total_bookings
    - total_spent
    - avg_booking_value
    - customer_lifetime_days
    - first_booking_date
    - last_booking_date
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating customer analytics")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'customer_analytics'):
        logger.info("Creating table gold.customer_analytics")
        iceberg_adapter.create_table('gold', 'customer_analytics', iceberg_schemas.GOLD_CUSTOMER_ANALYTICS_SCHEMA)
    
    # Read silver data
    customers = iceberg_adapter.read_table('silver', 'customer')
    bookings = iceberg_adapter.read_table('silver', 'booking')
    
    if customers is None or bookings is None:
        logger.warning("Missing data in Silver layer")
        return 0
    
    # Calculate customer metrics
    customer_stats = bookings.groupby('customer_id').agg(
        total_bookings=('booking_id', 'count'),
        total_spent=('booking_value', 'sum'),
        avg_booking_value=('booking_value', 'mean'),
        first_booking_date=('date', 'min'),
        last_booking_date=('date', 'max')
    ).reset_index()
    
    # Merge with customer data (drop total_bookings from customers to avoid conflict)
    customers_clean = customers.drop(columns=['total_bookings'], errors='ignore')
    customer_analytics = customers_clean.merge(customer_stats, on='customer_id', how='left')
    
    # Calculate customer lifetime days (convert dates to datetime first)
    customer_analytics['customer_lifetime_days'] = (
        pd.to_datetime(customer_analytics['last_booking_date']) - 
        pd.to_datetime(customer_analytics['first_booking_date'])
    ).dt.days
    
    # Add timestamp
    now = datetime.utcnow()
    customer_analytics['created_at'] = now
    customer_analytics['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        customer_analytics, 'gold', 'customer_analytics', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} customer analytics rows to Gold")
    return rows_written

"""Gold layer: Monthly user retention KPI."""

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


@task(name="aggregate-gold-monthly-retention", retries=2, retry_delay_seconds=30)
def aggregate_gold_monthly_retention(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate monthly user retention rate KPI.
    
    Business question: ¿Qué porcentaje de usuarios regresan el siguiente mes?
    
    Calculation: (Retained users / Users from previous month) × 100
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating monthly retention KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'monthly_retention'):
        logger.info("Creating table gold.monthly_retention")
        iceberg_adapter.create_table('gold', 'monthly_retention', iceberg_schemas.GOLD_MONTHLY_RETENTION_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    
    if bookings is None or len(bookings) == 0:
        logger.warning("No booking data in Silver layer")
        return 0
    
    # Extract year-month from date
    bookings['month'] = pd.to_datetime(bookings['date']).dt.to_period('M').astype(str)
    
    # Get unique users per month
    users_by_month = bookings.groupby('month')['customer_id'].apply(set).reset_index()
    users_by_month.columns = ['month', 'users']
    
    # Sort by month
    users_by_month = users_by_month.sort_values('month').reset_index(drop=True)
    
    # Calculate retention for each month
    retention_data = []
    
    for i in range(1, len(users_by_month)):
        current_month = users_by_month.loc[i, 'month']
        previous_month = users_by_month.loc[i-1, 'month']
        
        current_users = users_by_month.loc[i, 'users']
        previous_users = users_by_month.loc[i-1, 'users']
        
        # Users who appear in both months
        retained_users = len(current_users & previous_users)
        
        retention_data.append({
            'month': current_month,
            'previous_month': previous_month,
            'users_current_month': len(current_users),
            'users_previous_month': len(previous_users),
            'retained_users': retained_users,
            'retention_rate_pct': (retained_users / len(previous_users) * 100) if len(previous_users) > 0 else 0
        })
    
    if not retention_data:
        logger.warning("Not enough data to calculate retention (need at least 2 months)")
        return 0
    
    monthly_retention = pd.DataFrame(retention_data)
    
    # Add timestamps
    now = datetime.utcnow()
    monthly_retention['created_at'] = now
    monthly_retention['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        monthly_retention, 'gold', 'monthly_retention', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} monthly retention rows to Gold")
    return rows_written

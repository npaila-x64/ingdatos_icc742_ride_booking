"""Gold layer: Aggregate location analytics."""

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


@task(name="aggregate-gold-location-analytics", retries=2, retry_delay_seconds=30)
def aggregate_gold_location_analytics(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Create location analytics aggregation from Silver layer.
    
    Aggregates location-level metrics:
    - pickups (count)
    - dropoffs (count)
    - total_activity (pickups + dropoffs)
    - avg_booking_value
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating location analytics")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'location_analytics'):
        logger.info("Creating table gold.location_analytics")
        iceberg_adapter.create_table('gold', 'location_analytics', iceberg_schemas.GOLD_LOCATION_ANALYTICS_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    locations = iceberg_adapter.read_table('silver', 'location')
    
    if bookings is None or locations is None:
        logger.warning("Missing data in Silver layer")
        return 0
    
    # Calculate pickup location metrics
    pickup_stats = bookings.groupby('pickup_location_id').agg(
        pickups=('booking_id', 'count'),
        avg_booking_value=('booking_value', 'mean')
    ).reset_index()
    pickup_stats.rename(columns={'pickup_location_id': 'location_id'}, inplace=True)
    
    # Calculate dropoff location metrics
    dropoff_stats = bookings.groupby('drop_location_id').agg(
        dropoffs=('booking_id', 'count')
    ).reset_index()
    dropoff_stats.rename(columns={'drop_location_id': 'location_id'}, inplace=True)
    
    # Merge location analytics
    location_analytics = locations.merge(pickup_stats, on='location_id', how='left')
    location_analytics = location_analytics.merge(dropoff_stats, on='location_id', how='left')
    
    # Fill NaN values with 0
    location_analytics['pickups'] = location_analytics['pickups'].fillna(0).astype(int)
    location_analytics['dropoffs'] = location_analytics['dropoffs'].fillna(0).astype(int)
    location_analytics['avg_booking_value'] = location_analytics['avg_booking_value'].fillna(0)
    
    # Calculate total activity
    location_analytics['total_activity'] = location_analytics['pickups'] + location_analytics['dropoffs']
    
    # Add timestamp
    now = datetime.utcnow()
    location_analytics['created_at'] = now
    location_analytics['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        location_analytics, 'gold', 'location_analytics', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} location analytics rows to Gold")
    return rows_written

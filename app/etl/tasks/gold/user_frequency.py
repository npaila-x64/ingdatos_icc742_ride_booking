"""Gold layer: Customer satisfaction ratings KPI."""

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


@task(name="aggregate-gold-user-frequency", retries=2, retry_delay_seconds=30)
def aggregate_gold_user_frequency(
    iceberg_adapter: IcebergAdapter,
) -> int:
    """Calculate customer satisfaction based on ratings.
    
    Business question: ¿Qué tan satisfechos están los clientes con el servicio?
    
    Metrics:
    - Average customer rating (by customers)
    - Average driver rating (by drivers)
    - Distribution by satisfaction level
    - Overall satisfaction score
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        
    Returns:
        Number of rows written
    """
    logger.info("Aggregating customer satisfaction KPI")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('gold', 'user_frequency'):
        logger.info("Creating table gold.user_frequency")
        iceberg_adapter.create_table('gold', 'user_frequency', iceberg_schemas.GOLD_USER_FREQUENCY_SCHEMA)
    
    # Read silver data
    bookings = iceberg_adapter.read_table('silver', 'booking')
    rides = iceberg_adapter.read_table('silver', 'ride')
    
    if bookings is None or len(bookings) == 0:
        logger.warning("No booking data in Silver layer")
        return 0
    
    if rides is None or len(rides) == 0:
        logger.warning("No ride data in Silver layer")
        return 0
    
    # Get extraction_month from bronze.booking
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    if bronze_booking is not None and 'extraction_month' in bronze_booking.columns:
        extraction_mapping = bronze_booking[['booking_id', 'extraction_month']].drop_duplicates()
        bookings = bookings.merge(extraction_mapping, on='booking_id', how='left')
    
    # Merge bookings with rides to get ratings
    bookings_with_ratings = bookings.merge(
        rides[['booking_id', 'customer_rating', 'driver_rating']],
        on='booking_id',
        how='inner'
    )
    
    # Filter only rides with ratings
    rated_rides = bookings_with_ratings[
        bookings_with_ratings['customer_rating'].notna() | 
        bookings_with_ratings['driver_rating'].notna()
    ].copy()
    
    if len(rated_rides) == 0:
        logger.warning("No rated rides found")
        return 0
    
    # Group by date
    groupby_cols = ['date']
    if 'extraction_month' in rated_rides.columns:
        groupby_cols.append('extraction_month')
    
    # Calculate satisfaction metrics
    satisfaction = rated_rides.groupby(groupby_cols).agg(
        total_rated_rides=('booking_id', 'count'),
        avg_customer_rating=('customer_rating', 'mean'),
        avg_driver_rating=('driver_rating', 'mean')
    ).reset_index()
    
    # Classify rides by satisfaction level (using customer rating)
    def classify_rating(df):
        ratings = df['customer_rating'].dropna()
        return pd.Series({
            'excellent_rides': (ratings >= 4.5).sum(),
            'good_rides': ((ratings >= 3.5) & (ratings < 4.5)).sum(),
            'fair_rides': ((ratings >= 2.5) & (ratings < 3.5)).sum(),
            'poor_rides': (ratings < 2.5).sum()
        })
    
    rating_distribution = rated_rides.groupby(groupby_cols).apply(classify_rating).reset_index()
    
    # Merge with satisfaction metrics
    satisfaction = satisfaction.merge(rating_distribution, on=groupby_cols, how='left')
    
    # Calculate overall satisfaction score (0-100)
    # Formula: (excellent*100 + good*75 + fair*50 + poor*25) / total_rides
    satisfaction['satisfaction_score'] = (
        (satisfaction['excellent_rides'] * 100 + 
         satisfaction['good_rides'] * 75 + 
         satisfaction['fair_rides'] * 50 + 
         satisfaction['poor_rides'] * 25) / satisfaction['total_rated_rides']
    ).fillna(0)
    
    # Add timestamps
    now = datetime.utcnow()
    satisfaction['created_at'] = now
    satisfaction['updated_at'] = now
    
    # Write to Gold
    rows_written = iceberg_adapter.write_dataframe(
        satisfaction, 'gold', 'user_frequency', mode='overwrite'
    )
    
    logger.info(f"Aggregated {rows_written} customer satisfaction rows to Gold")
    logger.info(f"Avg satisfaction score: {satisfaction['satisfaction_score'].mean():.2f}")
    return rows_written

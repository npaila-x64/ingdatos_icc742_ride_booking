"""Gold layer: Aggregate Silver data into analytics-ready tables."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from prefect import task
from sqlalchemy import text

from app.adapters.postgresql import PostgreSQLAdapter

logger = logging.getLogger(__name__)


def _clean_row_for_insert(row: pd.Series) -> dict:
    """Convert pandas Series to dict, replacing NaN/inf with None."""
    row_dict = row.to_dict()
    for key, value in row_dict.items():
        if pd.isna(value) or (isinstance(value, (float, np.floating)) and not np.isfinite(value)):
            row_dict[key] = None
    return row_dict


@task(name="aggregate-to-gold", retries=2, retry_delay_seconds=30)
def aggregate_to_gold(
    db_adapter: PostgreSQLAdapter,
    target_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Aggregate Silver layer data into Gold layer analytics tables.
    
    Args:
        db_adapter: PostgreSQL database adapter
        target_date: Specific date to process. If None, processes all data.
        
    Returns:
        Dictionary with row counts for each Gold table
    """
    logger.info(f"Starting Gold aggregation for date: {target_date or 'all'}")
    
    row_counts = {}
    
    # Create aggregated analytics
    row_counts['gold.daily_booking_summary'] = _aggregate_daily_summary(db_adapter, target_date)
    row_counts['gold.customer_analytics'] = _aggregate_customer_analytics(db_adapter)
    row_counts['gold.location_analytics'] = _aggregate_location_analytics(db_adapter)
    
    logger.info(f"Gold aggregation completed. Row counts: {row_counts}")
    return row_counts


def _aggregate_daily_summary(
    db_adapter: PostgreSQLAdapter,
    target_date: Optional[datetime]
) -> int:
    """Aggregate daily booking summary statistics."""
    
    query = """
    SELECT
        b.date as summary_date,
        COUNT(DISTINCT b.booking_id) as total_bookings,
        COUNT(DISTINCT r.ride_id) as completed_rides,
        COUNT(DISTINCT cr.cancellation_id) as cancelled_rides,
        COUNT(DISTINCT ir.incompleted_id) as incompleted_rides,
        COALESCE(SUM(b.booking_value), 0) as total_revenue,
        AVG(r.ride_distance) as avg_ride_distance,
        AVG(r.driver_rating) as avg_driver_rating,
        AVG(r.customer_rating) as avg_customer_rating
    FROM silver.booking b
    LEFT JOIN silver.ride r ON b.booking_id = r.booking_id
    LEFT JOIN silver.cancelled_ride cr ON b.booking_id = cr.booking_id
    LEFT JOIN silver.incompleted_ride ir ON b.booking_id = ir.booking_id
    """
    
    if target_date:
        query += f" WHERE b.date = '{target_date.date()}'"
    
    query += " GROUP BY b.date ORDER BY b.date"
    
    summary_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(summary_df) == 0:
        logger.info("No data to aggregate for daily summary")
        return 0
    
    summary_df['created_at'] = datetime.now()
    summary_df['updated_at'] = datetime.now()
    
    # Upsert daily summaries
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in summary_df.iterrows():
            upsert_query = text("""
                INSERT INTO gold.daily_booking_summary (
                    summary_date, total_bookings, completed_rides, cancelled_rides,
                    incompleted_rides, total_revenue, avg_ride_distance,
                    avg_driver_rating, avg_customer_rating, created_at, updated_at
                )
                VALUES (
                    :summary_date, :total_bookings, :completed_rides, :cancelled_rides,
                    :incompleted_rides, :total_revenue, :avg_ride_distance,
                    :avg_driver_rating, :avg_customer_rating, :created_at, :updated_at
                )
                ON CONFLICT (summary_date) DO UPDATE SET
                    total_bookings = EXCLUDED.total_bookings,
                    completed_rides = EXCLUDED.completed_rides,
                    cancelled_rides = EXCLUDED.cancelled_rides,
                    incompleted_rides = EXCLUDED.incompleted_rides,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_ride_distance = EXCLUDED.avg_ride_distance,
                    avg_driver_rating = EXCLUDED.avg_driver_rating,
                    avg_customer_rating = EXCLUDED.avg_customer_rating,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Aggregated {rows_affected} daily summaries to Gold")
    return rows_affected


def _aggregate_customer_analytics(db_adapter: PostgreSQLAdapter) -> int:
    """Aggregate customer-level analytics."""
    
    query = """
    SELECT
        c.customer_id,
        c.total_bookings,
        COUNT(DISTINCT r.ride_id) as completed_rides,
        COUNT(DISTINCT cr.cancellation_id) as cancelled_rides,
        COALESCE(SUM(b.booking_value), 0) as total_spent,
        AVG(r.customer_rating) as avg_rating,
        MODE() WITHIN GROUP (ORDER BY vt.name) as favorite_vehicle_type,
        MIN(b.date) as first_booking_date,
        MAX(b.date) as last_booking_date
    FROM silver.customer c
    LEFT JOIN silver.booking b ON c.customer_id = b.customer_id
    LEFT JOIN silver.ride r ON b.booking_id = r.booking_id
    LEFT JOIN silver.cancelled_ride cr ON b.booking_id = cr.booking_id
    LEFT JOIN silver.vehicle_type vt ON b.vehicle_type_id = vt.vehicle_type_id
    GROUP BY c.customer_id, c.total_bookings
    """
    
    customer_analytics_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(customer_analytics_df) == 0:
        logger.info("No data to aggregate for customer analytics")
        return 0
    
    customer_analytics_df['created_at'] = datetime.now()
    customer_analytics_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in customer_analytics_df.iterrows():
            upsert_query = text("""
                INSERT INTO gold.customer_analytics (
                    customer_id, total_bookings, completed_rides, cancelled_rides,
                    total_spent, avg_rating, favorite_vehicle_type,
                    first_booking_date, last_booking_date, created_at, updated_at
                )
                VALUES (
                    :customer_id, :total_bookings, :completed_rides, :cancelled_rides,
                    :total_spent, :avg_rating, :favorite_vehicle_type,
                    :first_booking_date, :last_booking_date, :created_at, :updated_at
                )
                ON CONFLICT (customer_id) DO UPDATE SET
                    total_bookings = EXCLUDED.total_bookings,
                    completed_rides = EXCLUDED.completed_rides,
                    cancelled_rides = EXCLUDED.cancelled_rides,
                    total_spent = EXCLUDED.total_spent,
                    avg_rating = EXCLUDED.avg_rating,
                    favorite_vehicle_type = EXCLUDED.favorite_vehicle_type,
                    first_booking_date = EXCLUDED.first_booking_date,
                    last_booking_date = EXCLUDED.last_booking_date,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Aggregated {rows_affected} customer analytics to Gold")
    return rows_affected


def _aggregate_location_analytics(db_adapter: PostgreSQLAdapter) -> int:
    """Aggregate location-level analytics."""
    
    query = """
    SELECT
        l.location_id,
        l.name as location_name,
        COUNT(DISTINCT b_pickup.booking_id) as total_pickups,
        COUNT(DISTINCT b_drop.booking_id) as total_drops,
        AVG(b_pickup.booking_value) as avg_booking_value
    FROM silver.location l
    LEFT JOIN silver.booking b_pickup ON l.location_id = b_pickup.pickup_location_id
    LEFT JOIN silver.booking b_drop ON l.location_id = b_drop.drop_location_id
    GROUP BY l.location_id, l.name
    """
    
    location_analytics_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(location_analytics_df) == 0:
        logger.info("No data to aggregate for location analytics")
        return 0
    
    location_analytics_df['created_at'] = datetime.now()
    location_analytics_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in location_analytics_df.iterrows():
            upsert_query = text("""
                INSERT INTO gold.location_analytics (
                    location_id, location_name, total_pickups, total_drops,
                    avg_booking_value, created_at, updated_at
                )
                VALUES (
                    :location_id, :location_name, :total_pickups, :total_drops,
                    :avg_booking_value, :created_at, :updated_at
                )
                ON CONFLICT (location_id) DO UPDATE SET
                    location_name = EXCLUDED.location_name,
                    total_pickups = EXCLUDED.total_pickups,
                    total_drops = EXCLUDED.total_drops,
                    avg_booking_value = EXCLUDED.avg_booking_value,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Aggregated {rows_affected} location analytics to Gold")
    return rows_affected

"""Silver layer: Transform Bronze data into normalized dimensional model."""

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


@task(name="transform-to-silver", retries=2, retry_delay_seconds=30)
def transform_to_silver(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str] = None,
) -> dict[str, int]:
    """Transform Bronze layer data into Silver layer normalized tables.
    
    Args:
        db_adapter: PostgreSQL database adapter
        extraction_month: Specific month to process (format: YYYY-MM). If None, processes all.
        
    Returns:
        Dictionary with row counts for each Silver table
    """
    logger.info(f"Starting Silver transformation for month: {extraction_month or 'all'}")
    
    row_counts = {}
    
    # Transform dimensions first (referenced by facts)
    row_counts['silver.customer'] = _transform_customer_dimension(db_adapter, extraction_month)
    row_counts['silver.vehicle_type'] = _transform_vehicle_type_dimension(db_adapter, extraction_month)
    row_counts['silver.location'] = _transform_location_dimension(db_adapter, extraction_month)
    row_counts['silver.booking_status'] = _transform_booking_status_dimension(db_adapter, extraction_month)
    row_counts['silver.payment_method'] = _transform_payment_method_dimension(db_adapter, extraction_month)
    
    # Transform fact tables
    row_counts['silver.booking'] = _transform_booking_fact(db_adapter, extraction_month)
    row_counts['silver.ride'] = _transform_ride_fact(db_adapter, extraction_month)
    row_counts['silver.cancelled_ride'] = _transform_cancelled_ride_fact(db_adapter, extraction_month)
    row_counts['silver.incompleted_ride'] = _transform_incompleted_ride_fact(db_adapter, extraction_month)
    
    logger.info(f"Silver transformation completed. Row counts: {row_counts}")
    return row_counts


def _transform_customer_dimension(
    db_adapter: PostgreSQLAdapter, 
    extraction_month: Optional[str]
) -> int:
    """Transform customer data into Silver dimension using SCD Type 1."""
    
    # Get unique customers from Bronze
    query = """
    SELECT DISTINCT
        customer_id,
        MIN(extraction_date) as first_seen_date,
        MAX(extraction_date) as last_seen_date,
        COUNT(DISTINCT booking_id) as total_bookings
    FROM bronze.customer
    WHERE customer_id IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND extraction_month = '{extraction_month}'"
    
    query += " GROUP BY customer_id"
    
    customers_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(customers_df) == 0:
        logger.info("No customers to transform")
        return 0
    
    customers_df['created_at'] = datetime.now()
    customers_df['updated_at'] = datetime.now()
    
    # Upsert: insert new customers or update existing ones
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in customers_df.iterrows():
            upsert_query = text("""
                INSERT INTO silver.customer (customer_id, first_seen_date, last_seen_date, total_bookings, created_at, updated_at)
                VALUES (:customer_id, :first_seen_date, :last_seen_date, :total_bookings, :created_at, :updated_at)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_seen_date = LEAST(silver.customer.first_seen_date, EXCLUDED.first_seen_date),
                    last_seen_date = GREATEST(silver.customer.last_seen_date, EXCLUDED.last_seen_date),
                    total_bookings = silver.customer.total_bookings + EXCLUDED.total_bookings,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} customers to Silver")
    return rows_affected


def _transform_vehicle_type_dimension(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform vehicle type data into Silver dimension."""
    
    query = """
    SELECT DISTINCT vehicle_type_name as name
    FROM bronze.vehicle_type
    WHERE vehicle_type_name IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND extraction_month = '{extraction_month}'"
    
    vehicle_types_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(vehicle_types_df) == 0:
        logger.info("No vehicle types to transform")
        return 0
    
    vehicle_types_df['created_at'] = datetime.now()
    vehicle_types_df['updated_at'] = datetime.now()
    
    # Insert only new vehicle types (ignore duplicates)
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in vehicle_types_df.iterrows():
            insert_query = text("""
                INSERT INTO silver.vehicle_type (name, created_at, updated_at)
                VALUES (:name, :created_at, :updated_at)
                ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
            """)
            conn.execute(insert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} vehicle types to Silver")
    return rows_affected


def _transform_location_dimension(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform location data into Silver dimension."""
    
    query = """
    SELECT DISTINCT location_name as name
    FROM bronze.location
    WHERE location_name IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND extraction_month = '{extraction_month}'"
    
    locations_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(locations_df) == 0:
        logger.info("No locations to transform")
        return 0
    
    locations_df['created_at'] = datetime.now()
    locations_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in locations_df.iterrows():
            insert_query = text("""
                INSERT INTO silver.location (name, created_at, updated_at)
                VALUES (:name, :created_at, :updated_at)
                ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
            """)
            conn.execute(insert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} locations to Silver")
    return rows_affected


def _transform_booking_status_dimension(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform booking status data into Silver dimension."""
    
    query = """
    SELECT DISTINCT booking_status_name as name
    FROM bronze.booking_status
    WHERE booking_status_name IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND extraction_month = '{extraction_month}'"
    
    statuses_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(statuses_df) == 0:
        logger.info("No booking statuses to transform")
        return 0
    
    statuses_df['created_at'] = datetime.now()
    statuses_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in statuses_df.iterrows():
            insert_query = text("""
                INSERT INTO silver.booking_status (name, created_at, updated_at)
                VALUES (:name, :created_at, :updated_at)
                ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
            """)
            conn.execute(insert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} booking statuses to Silver")
    return rows_affected


def _transform_payment_method_dimension(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform payment method data into Silver dimension."""
    
    query = """
    SELECT DISTINCT payment_method_name as name
    FROM bronze.payment_method
    WHERE payment_method_name IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND extraction_month = '{extraction_month}'"
    
    methods_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(methods_df) == 0:
        logger.info("No payment methods to transform")
        return 0
    
    methods_df['created_at'] = datetime.now()
    methods_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in methods_df.iterrows():
            insert_query = text("""
                INSERT INTO silver.payment_method (name, created_at, updated_at)
                VALUES (:name, :created_at, :updated_at)
                ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
            """)
            conn.execute(insert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} payment methods to Silver")
    return rows_affected


def _transform_booking_fact(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform booking data into Silver fact table with foreign key references."""
    
    query = """
    SELECT DISTINCT
        b.booking_id,
        b.booking_date as date,
        b.booking_time as time,
        b.booking_value,
        b.customer_id,
        vt.vehicle_type_id,
        pl.location_id as pickup_location_id,
        dl.location_id as drop_location_id,
        bs.booking_status_id,
        pm.payment_method_id
    FROM bronze.booking b
    LEFT JOIN silver.vehicle_type vt ON b.vehicle_type = vt.name
    LEFT JOIN silver.location pl ON b.pickup_location = pl.name
    LEFT JOIN silver.location dl ON b.drop_location = dl.name
    LEFT JOIN silver.booking_status bs ON b.booking_status = bs.name
    LEFT JOIN silver.payment_method pm ON b.payment_method = pm.name
    WHERE b.booking_id IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND b.extraction_month = '{extraction_month}'"
    
    bookings_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(bookings_df) == 0:
        logger.info("No bookings to transform")
        return 0
    
    bookings_df['created_at'] = datetime.now()
    bookings_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in bookings_df.iterrows():
            upsert_query = text("""
                INSERT INTO silver.booking (
                    booking_id, date, time, booking_value, customer_id,
                    pickup_location_id, drop_location_id, vehicle_type_id,
                    booking_status_id, payment_method_id, created_at, updated_at
                )
                VALUES (
                    :booking_id, :date, :time, :booking_value, :customer_id,
                    :pickup_location_id, :drop_location_id, :vehicle_type_id,
                    :booking_status_id, :payment_method_id, :created_at, :updated_at
                )
                ON CONFLICT (booking_id) DO UPDATE SET
                    date = EXCLUDED.date,
                    time = EXCLUDED.time,
                    booking_value = EXCLUDED.booking_value,
                    customer_id = EXCLUDED.customer_id,
                    pickup_location_id = EXCLUDED.pickup_location_id,
                    drop_location_id = EXCLUDED.drop_location_id,
                    vehicle_type_id = EXCLUDED.vehicle_type_id,
                    booking_status_id = EXCLUDED.booking_status_id,
                    payment_method_id = EXCLUDED.payment_method_id,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} bookings to Silver")
    return rows_affected


def _transform_ride_fact(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform ride data into Silver fact table."""
    
    query = """
    SELECT DISTINCT
        r.booking_id,
        r.avg_vtat,
        r.avg_ctat,
        r.ride_distance,
        r.driver_rating,
        r.customer_rating
    FROM bronze.ride r
    INNER JOIN silver.booking b ON r.booking_id = b.booking_id
    WHERE r.booking_id IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND r.extraction_month = '{extraction_month}'"
    
    rides_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(rides_df) == 0:
        logger.info("No rides to transform")
        return 0
    
    rides_df['created_at'] = datetime.now()
    rides_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in rides_df.iterrows():
            upsert_query = text("""
                INSERT INTO silver.ride (
                    booking_id, avg_vtat, avg_ctat, ride_distance,
                    driver_rating, customer_rating, created_at, updated_at
                )
                VALUES (
                    :booking_id, :avg_vtat, :avg_ctat, :ride_distance,
                    :driver_rating, :customer_rating, :created_at, :updated_at
                )
                ON CONFLICT (booking_id) DO UPDATE SET
                    avg_vtat = EXCLUDED.avg_vtat,
                    avg_ctat = EXCLUDED.avg_ctat,
                    ride_distance = EXCLUDED.ride_distance,
                    driver_rating = EXCLUDED.driver_rating,
                    customer_rating = EXCLUDED.customer_rating,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} rides to Silver")
    return rows_affected


def _transform_cancelled_ride_fact(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform cancelled ride data into Silver fact table."""
    
    query = """
    SELECT DISTINCT
        cr.booking_id,
        cr.cancelled_by as ride_cancelled_by,
        cr.cancellation_reason
    FROM bronze.cancelled_ride cr
    INNER JOIN silver.booking b ON cr.booking_id = b.booking_id
    WHERE cr.booking_id IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND cr.extraction_month = '{extraction_month}'"
    
    cancelled_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(cancelled_df) == 0:
        logger.info("No cancelled rides to transform")
        return 0
    
    cancelled_df['created_at'] = datetime.now()
    cancelled_df['updated_at'] = datetime.now()
    
    # For cancelled rides, we might have multiple cancellations per booking (customer + driver)
    # So we insert without conflict handling, or use a composite unique key
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in cancelled_df.iterrows():
            # Check if this combination exists
            check_query = text("""
                SELECT cancellation_id FROM silver.cancelled_ride 
                WHERE booking_id = :booking_id AND ride_cancelled_by = :ride_cancelled_by
            """)
            result = conn.execute(check_query, {
                'booking_id': row['booking_id'],
                'ride_cancelled_by': row['ride_cancelled_by']
            })
            
            if result.fetchone() is None:
                insert_query = text("""
                    INSERT INTO silver.cancelled_ride (
                        booking_id, ride_cancelled_by, cancellation_reason, created_at, updated_at
                    )
                    VALUES (
                        :booking_id, :ride_cancelled_by, :cancellation_reason, :created_at, :updated_at
                    )
                """)
                conn.execute(insert_query, _clean_row_for_insert(row))
                rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} cancelled rides to Silver")
    return rows_affected


def _transform_incompleted_ride_fact(
    db_adapter: PostgreSQLAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform incompleted ride data into Silver fact table."""
    
    query = """
    SELECT DISTINCT
        ir.booking_id,
        ir.incompletion_reason
    FROM bronze.incompleted_ride ir
    INNER JOIN silver.booking b ON ir.booking_id = b.booking_id
    WHERE ir.booking_id IS NOT NULL
    """
    
    if extraction_month:
        query += f" AND ir.extraction_month = '{extraction_month}'"
    
    incompleted_df = pd.read_sql(text(query), db_adapter.engine)
    
    if len(incompleted_df) == 0:
        logger.info("No incompleted rides to transform")
        return 0
    
    incompleted_df['created_at'] = datetime.now()
    incompleted_df['updated_at'] = datetime.now()
    
    rows_affected = 0
    with db_adapter.engine.begin() as conn:
        for _, row in incompleted_df.iterrows():
            upsert_query = text("""
                INSERT INTO silver.incompleted_ride (
                    booking_id, incompletion_reason, created_at, updated_at
                )
                VALUES (
                    :booking_id, :incompletion_reason, :created_at, :updated_at
                )
                ON CONFLICT (booking_id) DO UPDATE SET
                    incompletion_reason = EXCLUDED.incompletion_reason,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.execute(upsert_query, _clean_row_for_insert(row))
            rows_affected += 1
    
    logger.info(f"Transformed {rows_affected} incompleted rides to Silver")
    return rows_affected

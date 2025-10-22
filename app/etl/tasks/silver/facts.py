"""Silver layer: Transform Bronze fact data into normalized Silver facts with FK relationships."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import pandas as pd
from prefect import task

from app.adapters import iceberg_schemas

if TYPE_CHECKING:
    from app.adapters.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)


@task(name="transform-silver-booking", retries=2, retry_delay_seconds=30)
def transform_silver_booking(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform booking fact from Bronze to Silver with dimension key lookups.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming booking fact")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'booking'):
        logger.info("Creating table silver.booking")
        iceberg_adapter.create_table('silver', 'booking', iceberg_schemas.SILVER_BOOKING_SCHEMA)
    
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    if bronze_booking is None or len(bronze_booking) == 0:
        logger.warning("No booking data in Bronze")
        return 0
    
    # Read dimension tables for FK lookups
    vehicle_types = iceberg_adapter.read_table('silver', 'vehicle_type')
    locations = iceberg_adapter.read_table('silver', 'location')
    booking_statuses = iceberg_adapter.read_table('silver', 'booking_status')
    payment_methods = iceberg_adapter.read_table('silver', 'payment_method')
    
    if any(x is None for x in [vehicle_types, locations, booking_statuses, payment_methods]):
        logger.error("Missing dimension tables in Silver layer")
        return 0
    
    # Create lookup dictionaries
    vehicle_lookup = dict(zip(vehicle_types['name'], vehicle_types['vehicle_type_id']))
    location_lookup = dict(zip(locations['name'], locations['location_id']))
    status_lookup = dict(zip(booking_statuses['name'], booking_statuses['booking_status_id']))
    payment_lookup = dict(zip(payment_methods['name'], payment_methods['payment_method_id']))
    
    # Map dimension keys
    bronze_booking['vehicle_type_id'] = bronze_booking['vehicle_type'].map(vehicle_lookup).astype('Int64')
    bronze_booking['pickup_location_id'] = bronze_booking['pickup_location'].map(location_lookup).astype('Int64')
    bronze_booking['drop_location_id'] = bronze_booking['drop_location'].map(location_lookup).astype('Int64')
    bronze_booking['booking_status_id'] = bronze_booking['booking_status'].map(status_lookup).astype('Int64')
    bronze_booking['payment_method_id'] = bronze_booking['payment_method'].map(payment_lookup).astype('Int64')
    
    silver_booking = bronze_booking[[
        'booking_id', 'customer_id', 'vehicle_type_id', 'pickup_location_id',
        'drop_location_id', 'booking_status_id', 'payment_method_id',
        'booking_value', 'date', 'time'
    ]].copy()
    
    now = datetime.utcnow()
    silver_booking['created_at'] = now
    silver_booking['updated_at'] = now
    
    rows_written = iceberg_adapter.write_dataframe(
        silver_booking, 'silver', 'booking', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} bookings to Silver")
    return rows_written


@task(name="transform-silver-ride", retries=2, retry_delay_seconds=30)
def transform_silver_ride(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform ride fact from Bronze to Silver.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming ride fact")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'ride'):
        logger.info("Creating table silver.ride")
        iceberg_adapter.create_table('silver', 'ride', iceberg_schemas.SILVER_RIDE_SCHEMA)
    
    bronze_ride = iceberg_adapter.read_table('bronze', 'ride')
    if bronze_ride is None or len(bronze_ride) == 0:
        logger.warning("No ride data in Bronze")
        return 0
    
    silver_ride = bronze_ride[[
        'booking_id', 'ride_distance', 'driver_rating', 'customer_rating'
    ]].copy()
    
    now = datetime.utcnow()
    silver_ride['created_at'] = now
    silver_ride['updated_at'] = now
    
    rows_written = iceberg_adapter.write_dataframe(
        silver_ride, 'silver', 'ride', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} rides to Silver")
    return rows_written


@task(name="transform-silver-cancelled-ride", retries=2, retry_delay_seconds=30)
def transform_silver_cancelled_ride(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform cancelled ride fact from Bronze to Silver.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming cancelled_ride fact")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'cancelled_ride'):
        logger.info("Creating table silver.cancelled_ride")
        iceberg_adapter.create_table('silver', 'cancelled_ride', iceberg_schemas.SILVER_CANCELLED_RIDE_SCHEMA)
    
    bronze_cancelled = iceberg_adapter.read_table('bronze', 'cancelled_ride')
    if bronze_cancelled is None or len(bronze_cancelled) == 0:
        logger.warning("No cancelled ride data in Bronze")
        return 0
    
    silver_cancelled = bronze_cancelled[[
        'booking_id', 'cancelled_rides_by_customer', 'cancelled_rides_by_driver'
    ]].copy()
    
    # Generate cancellation_id (row number)
    silver_cancelled['cancellation_id'] = range(1, len(silver_cancelled) + 1)
    
    now = datetime.utcnow()
    silver_cancelled['created_at'] = now
    silver_cancelled['updated_at'] = now
    
    # Reorder columns
    silver_cancelled = silver_cancelled[[
        'cancellation_id', 'booking_id', 'cancelled_rides_by_customer',
        'cancelled_rides_by_driver', 'created_at', 'updated_at'
    ]]
    
    rows_written = iceberg_adapter.write_dataframe(
        silver_cancelled, 'silver', 'cancelled_ride', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} cancelled rides to Silver")
    return rows_written


@task(name="transform-silver-incompleted-ride", retries=2, retry_delay_seconds=30)
def transform_silver_incompleted_ride(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform incompleted ride fact from Bronze to Silver.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming incompleted_ride fact")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'incompleted_ride'):
        logger.info("Creating table silver.incompleted_ride")
        iceberg_adapter.create_table('silver', 'incompleted_ride', iceberg_schemas.SILVER_INCOMPLETED_RIDE_SCHEMA)
    
    bronze_incompleted = iceberg_adapter.read_table('bronze', 'incompleted_ride')
    if bronze_incompleted is None or len(bronze_incompleted) == 0:
        logger.warning("No incompleted ride data in Bronze")
        return 0
    
    silver_incompleted = bronze_incompleted[[
        'booking_id', 'incomplete_rides', 'incomplete_rides_reason'
    ]].copy()
    
    # Generate incompleted_id (row number)
    silver_incompleted['incompleted_id'] = range(1, len(silver_incompleted) + 1)
    
    now = datetime.utcnow()
    silver_incompleted['created_at'] = now
    silver_incompleted['updated_at'] = now
    
    # Reorder columns
    silver_incompleted = silver_incompleted[[
        'incompleted_id', 'booking_id', 'incomplete_rides',
        'incomplete_rides_reason', 'created_at', 'updated_at'
    ]]
    
    rows_written = iceberg_adapter.write_dataframe(
        silver_incompleted, 'silver', 'incompleted_ride', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} incompleted rides to Silver")
    return rows_written

"""Silver layer: Transform Bronze dimension data into normalized Silver dimensions."""

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


@task(name="transform-silver-customer", retries=2, retry_delay_seconds=30)
def transform_silver_customer(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform customer data from Bronze to Silver with aggregated metrics.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming customer dimension")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'customer'):
        logger.info("Creating table silver.customer")
        iceberg_adapter.create_table('silver', 'customer', iceberg_schemas.SILVER_CUSTOMER_SCHEMA)
    
    bronze_customer = iceberg_adapter.read_table('bronze', 'customer')
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    
    if bronze_customer is None or bronze_booking is None or len(bronze_booking) == 0:
        logger.warning("No data in Bronze layer")
        return 0
    
    customer_metrics = bronze_booking.groupby('customer_id').agg(
        first_seen_date=('date', 'min'),
        last_seen_date=('date', 'max'),
        total_bookings=('booking_id', 'count')
    ).reset_index()
    
    now = datetime.utcnow()
    customer_metrics['created_at'] = now
    customer_metrics['updated_at'] = now
    
    rows_written = iceberg_adapter.write_dataframe(
        customer_metrics, 'silver', 'customer', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} customers to Silver")
    return rows_written


@task(name="transform-silver-vehicle-type", retries=2, retry_delay_seconds=30)
def transform_silver_vehicle_type(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform vehicle type data from Bronze to Silver with surrogate keys.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming vehicle_type dimension")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'vehicle_type'):
        logger.info("Creating table silver.vehicle_type")
        iceberg_adapter.create_table('silver', 'vehicle_type', iceberg_schemas.SILVER_VEHICLE_TYPE_SCHEMA)
    
    bronze_vehicle = iceberg_adapter.read_table('bronze', 'vehicle_type')
    if bronze_vehicle is None or len(bronze_vehicle) == 0:
        logger.warning("No vehicle type data in Bronze")
        return 0
    
    unique_vehicles = bronze_vehicle[['vehicle_type_name']].drop_duplicates()
    unique_vehicles = unique_vehicles[unique_vehicles['vehicle_type_name'].notna()]
    unique_vehicles['vehicle_type_id'] = range(1, len(unique_vehicles) + 1)
    unique_vehicles.rename(columns={'vehicle_type_name': 'name'}, inplace=True)
    
    now = datetime.utcnow()
    unique_vehicles['created_at'] = now
    unique_vehicles['updated_at'] = now
    unique_vehicles = unique_vehicles[['vehicle_type_id', 'name', 'created_at', 'updated_at']]
    
    rows_written = iceberg_adapter.write_dataframe(
        unique_vehicles, 'silver', 'vehicle_type', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} vehicle types to Silver")
    return rows_written


@task(name="transform-silver-location", retries=2, retry_delay_seconds=30)
def transform_silver_location(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform location data from Bronze to Silver with surrogate keys.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming location dimension")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'location'):
        logger.info("Creating table silver.location")
        iceberg_adapter.create_table('silver', 'location', iceberg_schemas.SILVER_LOCATION_SCHEMA)
    
    bronze_location = iceberg_adapter.read_table('bronze', 'location')
    if bronze_location is None or len(bronze_location) == 0:
        logger.warning("No location data in Bronze")
        return 0
    
    unique_locations = bronze_location[['location_name']].drop_duplicates()
    unique_locations = unique_locations[unique_locations['location_name'].notna()]
    unique_locations['location_id'] = range(1, len(unique_locations) + 1)
    unique_locations.rename(columns={'location_name': 'name'}, inplace=True)
    
    now = datetime.utcnow()
    unique_locations['created_at'] = now
    unique_locations['updated_at'] = now
    unique_locations = unique_locations[['location_id', 'name', 'created_at', 'updated_at']]
    
    rows_written = iceberg_adapter.write_dataframe(
        unique_locations, 'silver', 'location', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} locations to Silver")
    return rows_written


@task(name="transform-silver-booking-status", retries=2, retry_delay_seconds=30)
def transform_silver_booking_status(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform booking status data from Bronze to Silver with surrogate keys.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming booking_status dimension")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'booking_status'):
        logger.info("Creating table silver.booking_status")
        iceberg_adapter.create_table('silver', 'booking_status', iceberg_schemas.SILVER_BOOKING_STATUS_SCHEMA)
    
    bronze_status = iceberg_adapter.read_table('bronze', 'booking_status')
    if bronze_status is None or len(bronze_status) == 0:
        logger.warning("No booking status data in Bronze")
        return 0
    
    unique_statuses = bronze_status[['booking_status_name']].drop_duplicates()
    unique_statuses = unique_statuses[unique_statuses['booking_status_name'].notna()]
    unique_statuses['booking_status_id'] = range(1, len(unique_statuses) + 1)
    unique_statuses.rename(columns={'booking_status_name': 'name'}, inplace=True)
    
    now = datetime.utcnow()
    unique_statuses['created_at'] = now
    unique_statuses['updated_at'] = now
    unique_statuses = unique_statuses[['booking_status_id', 'name', 'created_at', 'updated_at']]
    
    rows_written = iceberg_adapter.write_dataframe(
        unique_statuses, 'silver', 'booking_status', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} booking statuses to Silver")
    return rows_written


@task(name="transform-silver-payment-method", retries=2, retry_delay_seconds=30)
def transform_silver_payment_method(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform payment method data from Bronze to Silver with surrogate keys.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Number of rows written
    """
    logger.info("Transforming payment_method dimension")
    
    # Initialize table if needed
    if not iceberg_adapter.table_exists('silver', 'payment_method'):
        logger.info("Creating table silver.payment_method")
        iceberg_adapter.create_table('silver', 'payment_method', iceberg_schemas.SILVER_PAYMENT_METHOD_SCHEMA)
    
    bronze_payment = iceberg_adapter.read_table('bronze', 'payment_method')
    if bronze_payment is None or len(bronze_payment) == 0:
        logger.warning("No payment method data in Bronze")
        return 0
    
    unique_payments = bronze_payment[['payment_method_name']].drop_duplicates()
    unique_payments = unique_payments[unique_payments['payment_method_name'].notna()]
    unique_payments['payment_method_id'] = range(1, len(unique_payments) + 1)
    unique_payments.rename(columns={'payment_method_name': 'name'}, inplace=True)
    
    now = datetime.utcnow()
    unique_payments['created_at'] = now
    unique_payments['updated_at'] = now
    unique_payments = unique_payments[['payment_method_id', 'name', 'created_at', 'updated_at']]
    
    rows_written = iceberg_adapter.write_dataframe(
        unique_payments, 'silver', 'payment_method', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} payment methods to Silver")
    return rows_written

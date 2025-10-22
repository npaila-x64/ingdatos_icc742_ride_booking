"""Silver layer: Transform Bronze data into normalized dimensional model for Iceberg."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.adapters import iceberg_schemas

logger = logging.getLogger(__name__)


def _initialize_silver_tables(iceberg_adapter: IcebergAdapter) -> None:
    """Initialize all Silver layer tables if they don't exist."""
    tables = {
        'customer': iceberg_schemas.SILVER_CUSTOMER_SCHEMA,
        'vehicle_type': iceberg_schemas.SILVER_VEHICLE_TYPE_SCHEMA,
        'location': iceberg_schemas.SILVER_LOCATION_SCHEMA,
        'booking_status': iceberg_schemas.SILVER_BOOKING_STATUS_SCHEMA,
        'payment_method': iceberg_schemas.SILVER_PAYMENT_METHOD_SCHEMA,
        'booking': iceberg_schemas.SILVER_BOOKING_SCHEMA,
        'ride': iceberg_schemas.SILVER_RIDE_SCHEMA,
        'cancelled_ride': iceberg_schemas.SILVER_CANCELLED_RIDE_SCHEMA,
        'incompleted_ride': iceberg_schemas.SILVER_INCOMPLETED_RIDE_SCHEMA,
    }
    
    for table_name, schema in tables.items():
        table = iceberg_adapter.get_table('silver', table_name)
        if table is None:
            logger.info(f"Creating table silver.{table_name}")
            iceberg_adapter.create_table('silver', table_name, schema)
        else:
            logger.info(f"Table silver.{table_name} already exists")


@task(name="transform-to-silver", retries=2, retry_delay_seconds=30)
def transform_to_silver(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> dict[str, int]:
    """Transform Bronze layer data into Silver layer normalized tables."""
    logger.info(f"Starting Silver transformation for month: {extraction_month or 'all'}")
    
    # Initialize Silver tables if they don't exist
    _initialize_silver_tables(iceberg_adapter)
    
    row_counts = {}
    
    # Transform dimensions first
    row_counts['silver.customer'] = _transform_customer_dimension(iceberg_adapter, extraction_month)
    row_counts['silver.vehicle_type'] = _transform_vehicle_type_dimension(iceberg_adapter, extraction_month)
    row_counts['silver.location'] = _transform_location_dimension(iceberg_adapter, extraction_month)
    row_counts['silver.booking_status'] = _transform_booking_status_dimension(iceberg_adapter, extraction_month)
    row_counts['silver.payment_method'] = _transform_payment_method_dimension(iceberg_adapter, extraction_month)
    
    # Transform fact tables
    row_counts['silver.booking'] = _transform_booking_fact(iceberg_adapter, extraction_month)
    row_counts['silver.ride'] = _transform_ride_fact(iceberg_adapter, extraction_month)
    row_counts['silver.cancelled_ride'] = _transform_cancelled_ride_fact(iceberg_adapter, extraction_month)
    row_counts['silver.incompleted_ride'] = _transform_incompleted_ride_fact(iceberg_adapter, extraction_month)
    
    logger.info(f"Silver transformation completed. Row counts: {row_counts}")
    return row_counts


def _transform_customer_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform customer data from Bronze to Silver."""
    logger.info("Transforming customer dimension")
    
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
    
    logger.info(f"Transformed {rows_written} customers")
    return rows_written


def _transform_vehicle_type_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform vehicle type data from Bronze to Silver."""
    logger.info("Transforming vehicle_type dimension")
    
    bronze_vehicle = iceberg_adapter.read_table('bronze', 'vehicle_type')
    if bronze_vehicle is None or len(bronze_vehicle) == 0:
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
    
    logger.info(f"Transformed {rows_written} vehicle types")
    return rows_written


def _transform_location_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform location data from Bronze to Silver."""
    logger.info("Transforming location dimension")
    
    bronze_location = iceberg_adapter.read_table('bronze', 'location')
    if bronze_location is None or len(bronze_location) == 0:
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
    
    logger.info(f"Transformed {rows_written} locations")
    return rows_written


def _transform_booking_status_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform booking status data from Bronze to Silver."""
    logger.info("Transforming booking_status dimension")
    
    bronze_status = iceberg_adapter.read_table('bronze', 'booking_status')
    if bronze_status is None or len(bronze_status) == 0:
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
    
    logger.info(f"Transformed {rows_written} booking statuses")
    return rows_written


def _transform_payment_method_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform payment method data from Bronze to Silver."""
    logger.info("Transforming payment_method dimension")
    
    bronze_payment = iceberg_adapter.read_table('bronze', 'payment_method')
    if bronze_payment is None or len(bronze_payment) == 0:
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
    
    logger.info(f"Transformed {rows_written} payment methods")
    return rows_written


def _transform_booking_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform booking data from Bronze to Silver with dimension key lookups."""
    logger.info("Transforming booking fact")
    
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    if bronze_booking is None or len(bronze_booking) == 0:
        return 0
    
    # Read dimension tables
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
    
    logger.info(f"Transformed {rows_written} bookings")
    return rows_written


def _transform_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform ride data from Bronze to Silver."""
    logger.info("Transforming ride fact")
    
    bronze_ride = iceberg_adapter.read_table('bronze', 'ride')
    if bronze_ride is None or len(bronze_ride) == 0:
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
    
    logger.info(f"Transformed {rows_written} rides")
    return rows_written


def _transform_cancelled_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform cancelled ride data from Bronze to Silver."""
    logger.info("Transforming cancelled_ride fact")
    
    bronze_cancelled = iceberg_adapter.read_table('bronze', 'cancelled_ride')
    if bronze_cancelled is None or len(bronze_cancelled) == 0:
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
    
    logger.info(f"Transformed {rows_written} cancelled rides")
    return rows_written


def _transform_incompleted_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> int:
    """Transform incompleted ride data from Bronze to Silver."""
    logger.info("Transforming incompleted_ride fact")
    
    bronze_incompleted = iceberg_adapter.read_table('bronze', 'incompleted_ride')
    if bronze_incompleted is None or len(bronze_incompleted) == 0:
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
    
    logger.info(f"Transformed {rows_written} incompleted rides")
    return rows_written
    
    rows_written = iceberg_adapter.write_dataframe(
        silver_incompleted, 'silver', 'incompleted_ride', mode='overwrite'
    )
    
    logger.info(f"Transformed {rows_written} incompleted rides")
    return rows_written

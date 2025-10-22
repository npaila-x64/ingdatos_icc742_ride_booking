"""Silver layer: Transform Bronze data into normalized dimensional model using Iceberg."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.adapters.iceberg_schemas import (
    SILVER_BOOKING_SCHEMA,
    SILVER_BOOKING_STATUS_SCHEMA,
    SILVER_CANCELLED_RIDE_SCHEMA,
    SILVER_CUSTOMER_SCHEMA,
    SILVER_INCOMPLETED_RIDE_SCHEMA,
    SILVER_LOCATION_SCHEMA,
    SILVER_PAYMENT_METHOD_SCHEMA,
    SILVER_RIDE_SCHEMA,
    SILVER_VEHICLE_TYPE_SCHEMA,
)

logger = logging.getLogger(__name__)


@task(name="transform-to-silver", retries=2, retry_delay_seconds=30)
def transform_to_silver(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> dict[str, int]:
    """Transform Bronze layer data into Silver layer normalized tables.
    
    Args:
        iceberg_adapter: Iceberg adapter
        extraction_month: Specific month to process (format: YYYY-MM). If None, processes all.
        
    Returns:
        Dictionary with row counts for each Silver table
    """
    logger.info(f"Starting Silver transformation for month: {extraction_month or 'all'}")
    
    row_counts = {}
    
    # Transform dimensions first (referenced by facts)
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
    extraction_month: Optional[str]
) -> int:
    """Transform customer data into Silver dimension using SCD Type 1."""
    
    # Read from Bronze
    bronze_customer = iceberg_adapter.read_table('bronze', 'customer')
    
    if len(bronze_customer) == 0:
        logger.info("No customers to transform")
        return 0
    
    # Filter by month if specified
    if extraction_month:
        bronze_customer = bronze_customer[bronze_customer['extraction_month'] == extraction_month]
    
    # Aggregate customer data
    customers_df = bronze_customer.groupby('customer_id').agg({
        'extraction_date': ['min', 'max'],
        'booking_id': 'nunique'
    }).reset_index()
    
    customers_df.columns = ['customer_id', 'first_seen_date', 'last_seen_date', 'total_bookings']
    customers_df['created_at'] = datetime.now()
    customers_df['updated_at'] = datetime.now()
    
    # Create or overwrite table
    if not iceberg_adapter.table_exists('silver', 'customer'):
        iceberg_adapter.create_table('silver', 'customer', SILVER_CUSTOMER_SCHEMA)
        mode = 'append'
    else:
        # For simplicity, we'll use overwrite mode for dimensions
        # In production, you'd want to implement proper upsert logic
        mode = 'overwrite'
    
    rows_written = iceberg_adapter.write_dataframe(customers_df, 'silver', 'customer', mode=mode)
    logger.info(f"Transformed {rows_written} customers to Silver")
    return rows_written


def _transform_vehicle_type_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform vehicle type data into Silver dimension."""
    
    bronze_vehicle = iceberg_adapter.read_table('bronze', 'vehicle_type')
    
    if len(bronze_vehicle) == 0:
        logger.info("No vehicle types to transform")
        return 0
    
    if extraction_month:
        bronze_vehicle = bronze_vehicle[bronze_vehicle['extraction_month'] == extraction_month]
    
    # Get unique vehicle types
    vehicle_types_df = bronze_vehicle[['vehicle_type_name']].drop_duplicates()
    vehicle_types_df.columns = ['name']
    vehicle_types_df['vehicle_type_id'] = range(1, len(vehicle_types_df) + 1)
    vehicle_types_df['created_at'] = datetime.now()
    vehicle_types_df['updated_at'] = datetime.now()
    
    # Reorder columns to match schema
    vehicle_types_df = vehicle_types_df[['vehicle_type_id', 'name', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'vehicle_type'):
        iceberg_adapter.create_table('silver', 'vehicle_type', SILVER_VEHICLE_TYPE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(vehicle_types_df, 'silver', 'vehicle_type', mode='overwrite')
    logger.info(f"Transformed {rows_written} vehicle types to Silver")
    return rows_written


def _transform_location_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform location data into Silver dimension."""
    
    bronze_location = iceberg_adapter.read_table('bronze', 'location')
    
    if len(bronze_location) == 0:
        logger.info("No locations to transform")
        return 0
    
    if extraction_month:
        bronze_location = bronze_location[bronze_location['extraction_month'] == extraction_month]
    
    # Get unique locations
    locations_df = bronze_location[['location_name']].drop_duplicates()
    locations_df.columns = ['name']
    locations_df['location_id'] = range(1, len(locations_df) + 1)
    locations_df['created_at'] = datetime.now()
    locations_df['updated_at'] = datetime.now()
    
    # Reorder columns
    locations_df = locations_df[['location_id', 'name', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'location'):
        iceberg_adapter.create_table('silver', 'location', SILVER_LOCATION_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(locations_df, 'silver', 'location', mode='overwrite')
    logger.info(f"Transformed {rows_written} locations to Silver")
    return rows_written


def _transform_booking_status_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform booking status data into Silver dimension."""
    
    bronze_status = iceberg_adapter.read_table('bronze', 'booking_status')
    
    if len(bronze_status) == 0:
        logger.info("No booking statuses to transform")
        return 0
    
    if extraction_month:
        bronze_status = bronze_status[bronze_status['extraction_month'] == extraction_month]
    
    statuses_df = bronze_status[['booking_status_name']].drop_duplicates()
    statuses_df.columns = ['name']
    statuses_df['booking_status_id'] = range(1, len(statuses_df) + 1)
    statuses_df['created_at'] = datetime.now()
    statuses_df['updated_at'] = datetime.now()
    
    statuses_df = statuses_df[['booking_status_id', 'name', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'booking_status'):
        iceberg_adapter.create_table('silver', 'booking_status', SILVER_BOOKING_STATUS_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(statuses_df, 'silver', 'booking_status', mode='overwrite')
    logger.info(f"Transformed {rows_written} booking statuses to Silver")
    return rows_written


def _transform_payment_method_dimension(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform payment method data into Silver dimension."""
    
    bronze_payment = iceberg_adapter.read_table('bronze', 'payment_method')
    
    if len(bronze_payment) == 0:
        logger.info("No payment methods to transform")
        return 0
    
    if extraction_month:
        bronze_payment = bronze_payment[bronze_payment['extraction_month'] == extraction_month]
    
    payment_df = bronze_payment[['payment_method_name']].drop_duplicates()
    payment_df.columns = ['name']
    payment_df['payment_method_id'] = range(1, len(payment_df) + 1)
    payment_df['created_at'] = datetime.now()
    payment_df['updated_at'] = datetime.now()
    
    payment_df = payment_df[['payment_method_id', 'name', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'payment_method'):
        iceberg_adapter.create_table('silver', 'payment_method', SILVER_PAYMENT_METHOD_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(payment_df, 'silver', 'payment_method', mode='overwrite')
    logger.info(f"Transformed {rows_written} payment methods to Silver")
    return rows_written


def _transform_booking_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform booking data into Silver fact table."""
    
    bronze_booking = iceberg_adapter.read_table('bronze', 'booking')
    
    if len(bronze_booking) == 0:
        logger.info("No bookings to transform")
        return 0
    
    if extraction_month:
        bronze_booking = bronze_booking[bronze_booking['extraction_month'] == extraction_month]
    
    # Load dimension tables for lookups
    vehicle_types = iceberg_adapter.read_table('silver', 'vehicle_type')
    locations = iceberg_adapter.read_table('silver', 'location')
    booking_statuses = iceberg_adapter.read_table('silver', 'booking_status')
    payment_methods = iceberg_adapter.read_table('silver', 'payment_method')
    
    # Join to get IDs
    booking_df = bronze_booking.copy()
    booking_df = booking_df.merge(
        vehicle_types[['vehicle_type_id', 'name']],
        left_on='vehicle_type',
        right_on='name',
        how='left'
    ).drop(columns=['name', 'vehicle_type'])
    
    booking_df = booking_df.merge(
        locations[['location_id', 'name']],
        left_on='pickup_location',
        right_on='name',
        how='left',
        suffixes=('', '_pickup')
    ).rename(columns={'location_id': 'pickup_location_id'}).drop(columns=['name', 'pickup_location'])
    
    booking_df = booking_df.merge(
        locations[['location_id', 'name']],
        left_on='drop_location',
        right_on='name',
        how='left',
        suffixes=('', '_drop')
    ).rename(columns={'location_id': 'drop_location_id'}).drop(columns=['name', 'drop_location'])
    
    booking_df = booking_df.merge(
        booking_statuses[['booking_status_id', 'name']],
        left_on='booking_status',
        right_on='name',
        how='left'
    ).drop(columns=['name', 'booking_status'])
    
    booking_df = booking_df.merge(
        payment_methods[['payment_method_id', 'name']],
        left_on='payment_method',
        right_on='name',
        how='left'
    ).drop(columns=['name', 'payment_method'])
    
    # Select and rename columns
    booking_df = booking_df[[
        'booking_id', 'customer_id', 'vehicle_type_id', 'pickup_location_id',
        'drop_location_id', 'booking_status_id', 'payment_method_id',
        'booking_value', 'date', 'time'
    ]]
    
    booking_df['created_at'] = datetime.now()
    booking_df['updated_at'] = datetime.now()
    
    if not iceberg_adapter.table_exists('silver', 'booking'):
        iceberg_adapter.create_table('silver', 'booking', SILVER_BOOKING_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(booking_df, 'silver', 'booking', mode='overwrite')
    logger.info(f"Transformed {rows_written} bookings to Silver")
    return rows_written


def _transform_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform ride data into Silver fact table."""
    
    bronze_ride = iceberg_adapter.read_table('bronze', 'ride')
    
    if len(bronze_ride) == 0:
        logger.info("No rides to transform")
        return 0
    
    if extraction_month:
        bronze_ride = bronze_ride[bronze_ride['extraction_month'] == extraction_month]
    
    ride_df = bronze_ride[['booking_id', 'ride_distance', 'driver_rating', 'customer_rating']].copy()
    ride_df['ride_id'] = range(1, len(ride_df) + 1)
    ride_df['created_at'] = datetime.now()
    ride_df['updated_at'] = datetime.now()
    
    ride_df = ride_df[['ride_id', 'booking_id', 'ride_distance', 'driver_rating', 'customer_rating', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'ride'):
        iceberg_adapter.create_table('silver', 'ride', SILVER_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(ride_df, 'silver', 'ride', mode='overwrite')
    logger.info(f"Transformed {rows_written} rides to Silver")
    return rows_written


def _transform_cancelled_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform cancelled ride data into Silver fact table."""
    
    bronze_cancelled = iceberg_adapter.read_table('bronze', 'cancelled_ride')
    
    if len(bronze_cancelled) == 0:
        logger.info("No cancelled rides to transform")
        return 0
    
    if extraction_month:
        bronze_cancelled = bronze_cancelled[bronze_cancelled['extraction_month'] == extraction_month]
    
    cancelled_df = bronze_cancelled[['booking_id', 'cancelled_rides_by_customer', 'cancelled_rides_by_driver']].copy()
    cancelled_df['cancellation_id'] = range(1, len(cancelled_df) + 1)
    cancelled_df['created_at'] = datetime.now()
    cancelled_df['updated_at'] = datetime.now()
    
    cancelled_df = cancelled_df[['cancellation_id', 'booking_id', 'cancelled_rides_by_customer', 'cancelled_rides_by_driver', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'cancelled_ride'):
        iceberg_adapter.create_table('silver', 'cancelled_ride', SILVER_CANCELLED_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(cancelled_df, 'silver', 'cancelled_ride', mode='overwrite')
    logger.info(f"Transformed {rows_written} cancelled rides to Silver")
    return rows_written


def _transform_incompleted_ride_fact(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str]
) -> int:
    """Transform incompleted ride data into Silver fact table."""
    
    bronze_incompleted = iceberg_adapter.read_table('bronze', 'incompleted_ride')
    
    if len(bronze_incompleted) == 0:
        logger.info("No incompleted rides to transform")
        return 0
    
    if extraction_month:
        bronze_incompleted = bronze_incompleted[bronze_incompleted['extraction_month'] == extraction_month]
    
    incompleted_df = bronze_incompleted[['booking_id', 'incomplete_rides', 'incomplete_rides_reason']].copy()
    incompleted_df['incompleted_id'] = range(1, len(incompleted_df) + 1)
    incompleted_df['created_at'] = datetime.now()
    incompleted_df['updated_at'] = datetime.now()
    
    incompleted_df = incompleted_df[['incompleted_id', 'booking_id', 'incomplete_rides', 'incomplete_rides_reason', 'created_at', 'updated_at']]
    
    if not iceberg_adapter.table_exists('silver', 'incompleted_ride'):
        iceberg_adapter.create_table('silver', 'incompleted_ride', SILVER_INCOMPLETED_RIDE_SCHEMA)
    
    rows_written = iceberg_adapter.write_dataframe(incompleted_df, 'silver', 'incompleted_ride', mode='overwrite')
    logger.info(f"Transformed {rows_written} incompleted rides to Silver")
    return rows_written

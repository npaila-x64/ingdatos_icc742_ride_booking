"""Bronze layer: Extract raw data from CSV and write to Iceberg tables partitioned by month."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.adapters.iceberg_schemas import (
    BRONZE_BOOKING_SCHEMA,
    BRONZE_BOOKING_STATUS_SCHEMA,
    BRONZE_CANCELLED_RIDE_SCHEMA,
    BRONZE_CUSTOMER_SCHEMA,
    BRONZE_INCOMPLETED_RIDE_SCHEMA,
    BRONZE_LOCATION_SCHEMA,
    BRONZE_PAYMENT_METHOD_SCHEMA,
    BRONZE_RIDE_SCHEMA,
    BRONZE_VEHICLE_TYPE_SCHEMA,
)

logger = logging.getLogger(__name__)


@task(name="extract-to-bronze", retries=2, retry_delay_seconds=30)
def extract_to_bronze(
    source_file: Path,
    iceberg_adapter: IcebergAdapter,
    extraction_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Extract data from source CSV and load into Bronze layer Iceberg tables partitioned by month.
    
    Args:
        source_file: Path to the source CSV file
        iceberg_adapter: Iceberg adapter
        extraction_date: Date of extraction (defaults to today)
        
    Returns:
        Dictionary with row counts for each Bronze table
    """
    extraction_date = extraction_date or datetime.now()
    
    logger.info(f"Starting Bronze extraction from {source_file}")
    
    # Read source CSV
    df = pd.read_csv(source_file)
    logger.info(f"Loaded {len(df)} rows from source file")
    
    # Clean column names
    df.columns = df.columns.str.strip()
    
    # Clean quoted string values
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace('"', '').str.strip()
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Add extraction metadata
    df['extraction_date'] = extraction_date.date()
    df['extraction_month'] = df['Date'].dt.strftime('%Y-%m')
    df['source_file'] = source_file.name
    
    row_counts = {}
    
    # Extract Customer
    row_counts['bronze.customer'] = _extract_customer(df, iceberg_adapter)
    
    # Extract Vehicle Type
    row_counts['bronze.vehicle_type'] = _extract_vehicle_type(df, iceberg_adapter)
    
    # Extract Location (pickup and drop)
    row_counts['bronze.location'] = _extract_location(df, iceberg_adapter)
    
    # Extract Booking Status
    row_counts['bronze.booking_status'] = _extract_booking_status(df, iceberg_adapter)
    
    # Extract Payment Method
    row_counts['bronze.payment_method'] = _extract_payment_method(df, iceberg_adapter)
    
    # Extract Booking (main fact)
    row_counts['bronze.booking'] = _extract_booking(df, iceberg_adapter)
    
    # Extract Ride (completed rides)
    row_counts['bronze.ride'] = _extract_ride(df, iceberg_adapter)
    
    # Extract Cancelled Rides
    row_counts['bronze.cancelled_ride'] = _extract_cancelled_ride(df, iceberg_adapter)
    
    # Extract Incompleted Rides
    row_counts['bronze.incompleted_ride'] = _extract_incompleted_ride(df, iceberg_adapter)
    
    logger.info(f"Bronze extraction completed. Row counts: {row_counts}")
    return row_counts


def _extract_customer(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract unique customers to Bronze layer."""
    customer_df = df[['Customer ID', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    customer_df.columns = ['customer_id', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    customer_df = customer_df.dropna(subset=['customer_id'])
    customer_df = customer_df.drop_duplicates(subset=['customer_id', 'booking_id', 'extraction_month'])
    
    # Ensure table exists
    if not iceberg_adapter.table_exists('bronze', 'customer'):
        iceberg_adapter.create_table('bronze', 'customer', BRONZE_CUSTOMER_SCHEMA)
    
    return iceberg_adapter.write_dataframe(customer_df, 'bronze', 'customer', mode='append')


def _extract_vehicle_type(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract vehicle types to Bronze layer."""
    vehicle_df = df[['Vehicle Type', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    vehicle_df.columns = ['vehicle_type_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    vehicle_df = vehicle_df.dropna(subset=['vehicle_type_name'])
    vehicle_df = vehicle_df.drop_duplicates(subset=['vehicle_type_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'vehicle_type'):
        iceberg_adapter.create_table('bronze', 'vehicle_type', BRONZE_VEHICLE_TYPE_SCHEMA)
    
    return iceberg_adapter.write_dataframe(vehicle_df, 'bronze', 'vehicle_type', mode='append')


def _extract_location(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract locations (pickup and drop) to Bronze layer."""
    # Pickup locations
    pickup_df = df[['Pickup Location', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    pickup_df.columns = ['location_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    pickup_df['location_type'] = 'pickup'
    pickup_df = pickup_df.dropna(subset=['location_name'])
    
    # Drop locations
    drop_df = df[['Drop Location', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    drop_df.columns = ['location_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    drop_df['location_type'] = 'drop'
    drop_df = drop_df.dropna(subset=['location_name'])
    
    # Combine and deduplicate
    location_df = pd.concat([pickup_df, drop_df], ignore_index=True)
    # Reorder columns to match schema
    location_df = location_df[['location_name', 'location_type', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']]
    location_df = location_df.drop_duplicates(subset=['location_name', 'location_type', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'location'):
        iceberg_adapter.create_table('bronze', 'location', BRONZE_LOCATION_SCHEMA)
    
    return iceberg_adapter.write_dataframe(location_df, 'bronze', 'location', mode='append')


def _extract_booking_status(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract booking statuses to Bronze layer."""
    status_df = df[['Booking Status', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    status_df.columns = ['booking_status_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    status_df = status_df.dropna(subset=['booking_status_name'])
    status_df = status_df.drop_duplicates(subset=['booking_status_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'booking_status'):
        iceberg_adapter.create_table('bronze', 'booking_status', BRONZE_BOOKING_STATUS_SCHEMA)
    
    return iceberg_adapter.write_dataframe(status_df, 'bronze', 'booking_status', mode='append')


def _extract_payment_method(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract payment methods to Bronze layer."""
    payment_df = df[['Payment Method', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    payment_df.columns = ['payment_method_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    payment_df = payment_df.dropna(subset=['payment_method_name'])
    payment_df = payment_df.drop_duplicates(subset=['payment_method_name', 'booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'payment_method'):
        iceberg_adapter.create_table('bronze', 'payment_method', BRONZE_PAYMENT_METHOD_SCHEMA)
    
    return iceberg_adapter.write_dataframe(payment_df, 'bronze', 'payment_method', mode='append')


def _extract_booking(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract booking records to Bronze layer."""
    booking_df = df[[
        'Booking ID', 'Customer ID', 'Vehicle Type', 'Pickup Location', 'Drop Location',
        'Booking Status', 'Payment Method', 'Booking Value', 'Date', 'Time',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    booking_df.columns = [
        'booking_id', 'customer_id', 'vehicle_type', 'pickup_location', 'drop_location',
        'booking_status', 'payment_method', 'booking_value', 'date', 'time',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    booking_df = booking_df.dropna(subset=['booking_id'])
    booking_df = booking_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'booking'):
        iceberg_adapter.create_table('bronze', 'booking', BRONZE_BOOKING_SCHEMA)
    
    return iceberg_adapter.write_dataframe(booking_df, 'bronze', 'booking', mode='append')


def _extract_ride(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract completed ride records to Bronze layer."""
    ride_df = df[df['Booking Status'] == 'Completed'][[
        'Booking ID', 'Ride Distance', 'Driver Ratings', 'Customer Rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    ride_df.columns = [
        'booking_id', 'ride_distance', 'driver_rating', 'customer_rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    ride_df = ride_df.dropna(subset=['booking_id'])
    ride_df = ride_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if not iceberg_adapter.table_exists('bronze', 'ride'):
        iceberg_adapter.create_table('bronze', 'ride', BRONZE_RIDE_SCHEMA)
    
    return iceberg_adapter.write_dataframe(ride_df, 'bronze', 'ride', mode='append')


def _extract_cancelled_ride(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract cancelled ride records to Bronze layer."""
    cancelled_df = df[(df['Cancelled Rides by Customer'].notna()) | (df['Cancelled Rides by Driver'].notna())][[
        'Booking ID', 'Cancelled Rides by Customer', 'Cancelled Rides by Driver',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    cancelled_df.columns = [
        'booking_id', 'cancelled_rides_by_customer', 'cancelled_rides_by_driver',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    cancelled_df = cancelled_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if len(cancelled_df) == 0:
        return 0
    
    # Convert to int (handling NaN)
    cancelled_df['cancelled_rides_by_customer'] = cancelled_df['cancelled_rides_by_customer'].fillna(0).astype(int)
    cancelled_df['cancelled_rides_by_driver'] = cancelled_df['cancelled_rides_by_driver'].fillna(0).astype(int)
    
    if not iceberg_adapter.table_exists('bronze', 'cancelled_ride'):
        iceberg_adapter.create_table('bronze', 'cancelled_ride', BRONZE_CANCELLED_RIDE_SCHEMA)
    
    return iceberg_adapter.write_dataframe(cancelled_df, 'bronze', 'cancelled_ride', mode='append')


def _extract_incompleted_ride(df: pd.DataFrame, iceberg_adapter: IcebergAdapter) -> int:
    """Extract incompleted ride records to Bronze layer."""
    incompleted_df = df[df['Incomplete Rides'].notna()][[
        'Booking ID', 'Incomplete Rides', 'Incomplete Rides Reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    incompleted_df.columns = [
        'booking_id', 'incomplete_rides', 'incomplete_rides_reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    incompleted_df = incompleted_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if len(incompleted_df) == 0:
        return 0
    
    # Convert to int
    incompleted_df['incomplete_rides'] = incompleted_df['incomplete_rides'].fillna(0).astype(int)
    
    if not iceberg_adapter.table_exists('bronze', 'incompleted_ride'):
        iceberg_adapter.create_table('bronze', 'incompleted_ride', BRONZE_INCOMPLETED_RIDE_SCHEMA)
    
    return iceberg_adapter.write_dataframe(incompleted_df, 'bronze', 'incompleted_ride', mode='append')

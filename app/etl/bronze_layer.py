"""Bronze layer: Extract raw data from CSV and partition by month."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import task

from app.adapters.postgresql import PostgreSQLAdapter

logger = logging.getLogger(__name__)


@task(name="extract-to-bronze", retries=2, retry_delay_seconds=30)
def extract_to_bronze(
    source_file: Path,
    db_adapter: PostgreSQLAdapter,
    extraction_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Extract data from source CSV and load into Bronze layer tables partitioned by month.
    
    Args:
        source_file: Path to the source CSV file
        db_adapter: PostgreSQL database adapter
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
    row_counts['bronze.customer'] = _extract_customer(df, db_adapter)
    
    # Extract Vehicle Type
    row_counts['bronze.vehicle_type'] = _extract_vehicle_type(df, db_adapter)
    
    # Extract Location (pickup and drop)
    row_counts['bronze.location'] = _extract_location(df, db_adapter)
    
    # Extract Booking Status
    row_counts['bronze.booking_status'] = _extract_booking_status(df, db_adapter)
    
    # Extract Payment Method
    row_counts['bronze.payment_method'] = _extract_payment_method(df, db_adapter)
    
    # Extract Booking (main fact)
    row_counts['bronze.booking'] = _extract_booking(df, db_adapter)
    
    # Extract Ride (completed rides)
    row_counts['bronze.ride'] = _extract_ride(df, db_adapter)
    
    # Extract Cancelled Rides
    row_counts['bronze.cancelled_ride'] = _extract_cancelled_ride(df, db_adapter)
    
    # Extract Incompleted Rides
    row_counts['bronze.incompleted_ride'] = _extract_incompleted_ride(df, db_adapter)
    
    logger.info(f"Bronze extraction completed. Row counts: {row_counts}")
    return row_counts


def _extract_customer(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract unique customers to Bronze layer."""
    customer_df = df[['Customer ID', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    customer_df.columns = ['customer_id', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    customer_df = customer_df.dropna(subset=['customer_id'])
    customer_df = customer_df.drop_duplicates(subset=['customer_id', 'booking_id', 'extraction_month'])
    
    return db_adapter.write_table(customer_df, 'customer', schema='bronze', if_exists='append')


def _extract_vehicle_type(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract vehicle types to Bronze layer."""
    vehicle_df = df[['Vehicle Type', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    vehicle_df.columns = ['vehicle_type_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    vehicle_df = vehicle_df.dropna(subset=['vehicle_type_name'])
    vehicle_df = vehicle_df.drop_duplicates(subset=['vehicle_type_name', 'booking_id', 'extraction_month'])
    
    return db_adapter.write_table(vehicle_df, 'vehicle_type', schema='bronze', if_exists='append')


def _extract_location(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
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
    location_df = location_df.drop_duplicates(subset=['location_name', 'location_type', 'booking_id', 'extraction_month'])
    
    return db_adapter.write_table(location_df, 'location', schema='bronze', if_exists='append')


def _extract_booking_status(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract booking statuses to Bronze layer."""
    status_df = df[['Booking Status', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    status_df.columns = ['booking_status_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    status_df = status_df.dropna(subset=['booking_status_name'])
    status_df = status_df.drop_duplicates(subset=['booking_status_name', 'booking_id', 'extraction_month'])
    
    return db_adapter.write_table(status_df, 'booking_status', schema='bronze', if_exists='append')


def _extract_payment_method(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract payment methods to Bronze layer."""
    payment_df = df[['Payment Method', 'Booking ID', 'extraction_date', 'extraction_month', 'source_file']].copy()
    payment_df.columns = ['payment_method_name', 'booking_id', 'extraction_date', 'extraction_month', 'source_file']
    payment_df = payment_df.dropna(subset=['payment_method_name'])
    payment_df = payment_df.drop_duplicates(subset=['payment_method_name', 'booking_id', 'extraction_month'])
    
    return db_adapter.write_table(payment_df, 'payment_method', schema='bronze', if_exists='append')


def _extract_booking(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract booking records to Bronze layer."""
    booking_df = df[[
        'Booking ID', 'Date', 'Time', 'Booking Status', 'Customer ID', 
        'Vehicle Type', 'Pickup Location', 'Drop Location', 
        'Booking Value', 'Payment Method',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    booking_df.columns = [
        'booking_id', 'booking_date', 'booking_time', 'booking_status', 'customer_id',
        'vehicle_type', 'pickup_location', 'drop_location',
        'booking_value', 'payment_method',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    booking_df = booking_df.dropna(subset=['booking_id'])
    booking_df = booking_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    return db_adapter.write_table(booking_df, 'booking', schema='bronze', if_exists='append')


def _extract_ride(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract completed ride records to Bronze layer."""
    ride_df = df[df['Booking Status'] == 'Completed'][[
        'Booking ID', 'Avg VTAT', 'Avg CTAT', 'Ride Distance',
        'Driver Ratings', 'Customer Rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    ride_df.columns = [
        'booking_id', 'avg_vtat', 'avg_ctat', 'ride_distance',
        'driver_rating', 'customer_rating',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    ride_df = ride_df.dropna(subset=['booking_id'])
    ride_df = ride_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    return db_adapter.write_table(ride_df, 'ride', schema='bronze', if_exists='append')


def _extract_cancelled_ride(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract cancelled ride records to Bronze layer."""
    # Customer cancellations
    customer_cancelled = df[df['Cancelled Rides by Customer'].notna()][[
        'Booking ID', 'extraction_date', 'extraction_month', 'source_file',
        'Reason for cancelling by Customer'
    ]].copy()
    customer_cancelled['cancelled_by'] = 'Customer'
    customer_cancelled.columns = [
        'booking_id', 'extraction_date', 'extraction_month', 'source_file',
        'cancellation_reason', 'cancelled_by'
    ]
    
    # Driver cancellations
    driver_cancelled = df[df['Cancelled Rides by Driver'].notna()][[
        'Booking ID', 'extraction_date', 'extraction_month', 'source_file',
        'Driver Cancellation Reason'
    ]].copy()
    driver_cancelled['cancelled_by'] = 'Driver'
    driver_cancelled.columns = [
        'booking_id', 'extraction_date', 'extraction_month', 'source_file',
        'cancellation_reason', 'cancelled_by'
    ]
    
    # Combine
    cancelled_df = pd.concat([customer_cancelled, driver_cancelled], ignore_index=True)
    cancelled_df = cancelled_df[['booking_id', 'cancelled_by', 'cancellation_reason', 
                                  'extraction_date', 'extraction_month', 'source_file']]
    cancelled_df = cancelled_df.drop_duplicates(subset=['booking_id', 'cancelled_by', 'extraction_month'])
    
    if len(cancelled_df) == 0:
        return 0
    
    return db_adapter.write_table(cancelled_df, 'cancelled_ride', schema='bronze', if_exists='append')


def _extract_incompleted_ride(df: pd.DataFrame, db_adapter: PostgreSQLAdapter) -> int:
    """Extract incompleted ride records to Bronze layer."""
    incompleted_df = df[df['Incomplete Rides'].notna()][[
        'Booking ID', 'Incomplete Rides Reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]].copy()
    
    incompleted_df.columns = [
        'booking_id', 'incompletion_reason',
        'extraction_date', 'extraction_month', 'source_file'
    ]
    
    incompleted_df = incompleted_df.drop_duplicates(subset=['booking_id', 'extraction_month'])
    
    if len(incompleted_df) == 0:
        return 0
    
    return db_adapter.write_table(incompleted_df, 'incompleted_ride', schema='bronze', if_exists='append')

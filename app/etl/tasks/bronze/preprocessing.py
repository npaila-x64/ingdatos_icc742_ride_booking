"""Shared preprocessing task for loading and preparing source data."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)


@task(name="load-and-prepare-source-data", retries=2, retry_delay_seconds=30)
def load_and_prepare_source_data(
    source_file: Path,
    extraction_date: Optional[datetime] = None,
    no_date_filter: bool = False,
) -> pd.DataFrame:
    """Load source CSV and add extraction metadata.
    
    Args:
        source_file: Path to the source CSV file
        extraction_date: Date of extraction (defaults to today)
        no_date_filter: If True, skip date filtering and process all data
        
    Returns:
        DataFrame with cleaned data and metadata columns
    """
    extraction_date = extraction_date or datetime.now()
    
    logger.info(f"Loading source data from {source_file}")
    
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
    
    # Filter data by extraction date month (unless disabled)
    if not no_date_filter:
        # Only include bookings from the same month/year as extraction_date
        extraction_year_month = extraction_date.strftime('%Y-%m')
        df['_temp_year_month'] = df['Date'].dt.strftime('%Y-%m')
        
        original_count = len(df)
        df = df[df['_temp_year_month'] == extraction_year_month].copy()
        df = df.drop(columns=['_temp_year_month'])
        
        filtered_count = len(df)
        logger.info(f"Filtered data by date: {original_count} → {filtered_count} rows (keeping only {extraction_year_month})")
    else:
        logger.info(f"Date filtering disabled - processing all {len(df)} rows")
    
    # Add extraction metadata
    df['extraction_date'] = extraction_date.date()
    df['extraction_month'] = extraction_date.strftime('%Y-%m')
    df['source_file'] = source_file.name
    
    logger.info(f"Prepared source data with extraction metadata")
    
    return df

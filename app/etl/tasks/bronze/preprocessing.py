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
) -> pd.DataFrame:
    """Load source CSV and add extraction metadata.
    
    Args:
        source_file: Path to the source CSV file
        extraction_date: Date of extraction (defaults to today)
        
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
    
    # Add extraction metadata
    df['extraction_date'] = extraction_date.date()
    df['extraction_month'] = df['Date'].dt.strftime('%Y-%m')
    df['source_file'] = source_file.name
    
    logger.info(f"Prepared source data with extraction metadata")
    
    return df

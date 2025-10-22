"""Main Prefect flow orchestrating the medallion architecture ETL pipeline with Apache Iceberg."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from prefect import flow, task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings
from app.etl.bronze_layer import extract_to_bronze
from app.etl.gold_layer_iceberg import aggregate_to_gold
from app.etl.silver_layer_iceberg import transform_to_silver

logger = logging.getLogger(__name__)


@task(name="initialize-iceberg-namespaces")
def initialize_iceberg_namespaces(iceberg_adapter: IcebergAdapter) -> None:
    """Ensure Iceberg namespaces (schemas) are created."""
    logger.info("Initializing Iceberg namespaces")
    
    # Create namespaces if they don't exist
    for namespace in ['bronze', 'silver', 'gold']:
        iceberg_adapter.create_namespace(namespace)
    
    logger.info("Iceberg namespaces initialized")


@flow(
    name="ride-booking-medallion-etl",
    description="ETL pipeline implementing medallion architecture for ride booking data with Apache Iceberg",
    log_prints=True,
)
def ride_booking_etl(
    source_file: Optional[str] = None,
    extraction_date: Optional[datetime] = None,
    run_bronze: bool = True,
    run_silver: bool = True,
    run_gold: bool = True,
) -> dict[str, dict[str, int]]:
    """Main ETL flow orchestrating Bronze -> Silver -> Gold transformations with Apache Iceberg.
    
    Args:
        source_file: Path to source CSV file (defaults to data/ncr_ride_bookings.csv)
        extraction_date: Date of extraction (defaults to today)
        run_bronze: Whether to run Bronze layer extraction
        run_silver: Whether to run Silver layer transformation
        run_gold: Whether to run Gold layer aggregation
        
    Returns:
        Dictionary with execution statistics for each layer
    """
    settings = load_settings()
    
    # Default to the project's main data file
    if source_file is None:
        source_file = str(settings.data_path / "ncr_ride_bookings.csv")
    
    source_path = Path(source_file)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")
    
    extraction_date = extraction_date or datetime.now()
    extraction_month = extraction_date.strftime('%Y-%m')
    
    logger.info(f"Starting ETL pipeline for {source_path.name}")
    logger.info(f"Extraction date: {extraction_date.date()}, Month: {extraction_month}")
    logger.info(f"Iceberg warehouse: {settings.iceberg.warehouse_path}")
    
    # Initialize Iceberg adapter
    iceberg_adapter = IcebergAdapter(settings.iceberg)
    
    # Initialize Iceberg namespaces
    initialize_iceberg_namespaces(iceberg_adapter)
    
    results = {}
    
    # Bronze Layer: Extract raw data
    if run_bronze:
        logger.info("=" * 80)
        logger.info("BRONZE LAYER: Extracting raw data to Iceberg tables")
        logger.info("=" * 80)
        bronze_results = extract_to_bronze(
            source_file=source_path,
            iceberg_adapter=iceberg_adapter,
            extraction_date=extraction_date,
        )
        results['bronze'] = bronze_results
        logger.info(f"Bronze extraction completed: {sum(bronze_results.values())} total rows")
    
    # Silver Layer: Transform to normalized model
    if run_silver:
        logger.info("=" * 80)
        logger.info("SILVER LAYER: Transforming to dimensional model in Iceberg")
        logger.info("=" * 80)
        silver_results = transform_to_silver(
            iceberg_adapter=iceberg_adapter,
            extraction_month=extraction_month if run_bronze else None,
        )
        results['silver'] = silver_results
        logger.info(f"Silver transformation completed: {sum(silver_results.values())} total rows")
    
    # Gold Layer: Aggregate analytics
    if run_gold:
        logger.info("=" * 80)
        logger.info("GOLD LAYER: Aggregating analytics in Iceberg")
        logger.info("=" * 80)
        gold_results = aggregate_to_gold(
            iceberg_adapter=iceberg_adapter,
            target_date=extraction_date if run_bronze else None,
        )
        results['gold'] = gold_results
        logger.info(f"Gold aggregation completed: {sum(gold_results.values())} total rows")
    
    logger.info("=" * 80)
    logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"Total results: {results}")
    logger.info(f"Data stored in: {settings.iceberg.warehouse_path}")
    
    return results


@flow(
    name="ride-booking-incremental-etl",
    description="Incremental ETL for processing new ride booking data",
)
def incremental_etl(
    source_file: str,
    extraction_date: Optional[datetime] = None,
) -> dict[str, dict[str, int]]:
    """Incremental ETL flow for processing new data files.
    
    This flow is optimized for processing new monthly data files.
    
    Args:
        source_file: Path to the new source CSV file
        extraction_date: Date of extraction (defaults to today)
        
    Returns:
        Dictionary with execution statistics for each layer
    """
    return ride_booking_etl(
        source_file=source_file,
        extraction_date=extraction_date,
        run_bronze=True,
        run_silver=True,
        run_gold=True,
    )


@flow(
    name="ride-booking-backfill-etl",
    description="Backfill ETL for reprocessing all historical data",
)
def backfill_etl() -> dict[str, dict[str, int]]:
    """Backfill flow for reprocessing all data in Silver and Gold layers.
    
    This flow skips Bronze extraction and reprocesses all existing Bronze data.
    
    Returns:
        Dictionary with execution statistics for each layer
    """
    return ride_booking_etl(
        source_file=None,
        extraction_date=None,
        run_bronze=False,  # Skip Bronze, use existing data
        run_silver=True,
        run_gold=True,
    )


if __name__ == "__main__":
    # Allow running the flow directly from command line
    ride_booking_etl()

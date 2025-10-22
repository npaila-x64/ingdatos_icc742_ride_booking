"""Granular Prefect flows orchestrating the medallion architecture ETL pipeline."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from prefect import flow, task

from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

# Import granular tasks
from app.etl.tasks.bronze import (
    load_and_prepare_source_data,
    extract_bronze_customer,
    extract_bronze_vehicle_type,
    extract_bronze_location,
    extract_bronze_booking_status,
    extract_bronze_payment_method,
    extract_bronze_booking,
    extract_bronze_ride,
    extract_bronze_cancelled_ride,
    extract_bronze_incompleted_ride,
)

from app.etl.tasks.silver import (
    transform_silver_customer,
    transform_silver_vehicle_type,
    transform_silver_location,
    transform_silver_booking_status,
    transform_silver_payment_method,
    transform_silver_booking,
    transform_silver_ride,
    transform_silver_cancelled_ride,
    transform_silver_incompleted_ride,
)

from app.etl.tasks.gold import (
    aggregate_gold_daily_booking_summary,
    aggregate_gold_customer_analytics,
    aggregate_gold_location_analytics,
)

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
    name="ride-booking-bronze-extraction",
    description="Extract raw data from source CSV into Bronze layer Iceberg tables",
    log_prints=True,
)
def bronze_extraction_flow(
    source_file: Path,
    iceberg_adapter: IcebergAdapter,
    extraction_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Run granular Bronze layer extraction.
    
    This flow orchestrates individual entity extractions in parallel where possible.
    
    Args:
        source_file: Path to source CSV file
        iceberg_adapter: Iceberg adapter instance
        extraction_date: Date of extraction
        
    Returns:
        Dictionary with row counts for each Bronze table
    """
    logger.info("=" * 80)
    logger.info("BRONZE LAYER: Extracting raw data to Iceberg tables")
    logger.info("=" * 80)
    
    # Step 1: Load and prepare source data (single task)
    prepared_df = load_and_prepare_source_data(source_file, extraction_date)
    
    # Step 2: Extract all entities in parallel
    # Group 1: Independent dimension extractions (can run in parallel)
    customer_count = extract_bronze_customer(prepared_df, iceberg_adapter)
    vehicle_type_count = extract_bronze_vehicle_type(prepared_df, iceberg_adapter)
    location_count = extract_bronze_location(prepared_df, iceberg_adapter)
    booking_status_count = extract_bronze_booking_status(prepared_df, iceberg_adapter)
    payment_method_count = extract_bronze_payment_method(prepared_df, iceberg_adapter)
    
    # Group 2: Fact extractions (can run in parallel after dimensions)
    booking_count = extract_bronze_booking(prepared_df, iceberg_adapter)
    ride_count = extract_bronze_ride(prepared_df, iceberg_adapter)
    cancelled_ride_count = extract_bronze_cancelled_ride(prepared_df, iceberg_adapter)
    incompleted_ride_count = extract_bronze_incompleted_ride(prepared_df, iceberg_adapter)
    
    results = {
        'bronze.customer': customer_count,
        'bronze.vehicle_type': vehicle_type_count,
        'bronze.location': location_count,
        'bronze.booking_status': booking_status_count,
        'bronze.payment_method': payment_method_count,
        'bronze.booking': booking_count,
        'bronze.ride': ride_count,
        'bronze.cancelled_ride': cancelled_ride_count,
        'bronze.incompleted_ride': incompleted_ride_count,
    }
    
    logger.info(f"Bronze extraction completed: {sum(results.values())} total rows")
    return results


@flow(
    name="ride-booking-silver-transformation",
    description="Transform Bronze data into normalized Silver dimensional model",
    log_prints=True,
)
def silver_transformation_flow(
    iceberg_adapter: IcebergAdapter,
    extraction_month: Optional[str] = None,
) -> dict[str, int]:
    """Run granular Silver layer transformations.
    
    This flow orchestrates dimension and fact transformations with proper dependencies.
    Dimensions must complete before facts can run.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        extraction_month: Optional month filter (YYYY-MM format)
        
    Returns:
        Dictionary with row counts for each Silver table
    """
    logger.info("=" * 80)
    logger.info("SILVER LAYER: Transforming to dimensional model in Iceberg")
    logger.info("=" * 80)
    
    # Step 1: Transform all dimensions in parallel
    customer_count = transform_silver_customer(iceberg_adapter, extraction_month)
    vehicle_type_count = transform_silver_vehicle_type(iceberg_adapter, extraction_month)
    location_count = transform_silver_location(iceberg_adapter, extraction_month)
    booking_status_count = transform_silver_booking_status(iceberg_adapter, extraction_month)
    payment_method_count = transform_silver_payment_method(iceberg_adapter, extraction_month)
    
    # Step 2: Transform facts (depend on dimensions, can run in parallel after dimensions complete)
    # Prefect will automatically wait for dimensions to complete
    booking_count = transform_silver_booking(iceberg_adapter, extraction_month)
    ride_count = transform_silver_ride(iceberg_adapter, extraction_month)
    cancelled_ride_count = transform_silver_cancelled_ride(iceberg_adapter, extraction_month)
    incompleted_ride_count = transform_silver_incompleted_ride(iceberg_adapter, extraction_month)
    
    results = {
        'silver.customer': customer_count,
        'silver.vehicle_type': vehicle_type_count,
        'silver.location': location_count,
        'silver.booking_status': booking_status_count,
        'silver.payment_method': payment_method_count,
        'silver.booking': booking_count,
        'silver.ride': ride_count,
        'silver.cancelled_ride': cancelled_ride_count,
        'silver.incompleted_ride': incompleted_ride_count,
    }
    
    logger.info(f"Silver transformation completed: {sum(results.values())} total rows")
    return results


@flow(
    name="ride-booking-gold-aggregation",
    description="Aggregate Silver data into Gold analytics tables",
    log_prints=True,
)
def gold_aggregation_flow(
    iceberg_adapter: IcebergAdapter,
    target_date: Optional[datetime] = None,
) -> dict[str, int]:
    """Run granular Gold layer aggregations.
    
    This flow orchestrates analytics aggregations in parallel.
    
    Args:
        iceberg_adapter: Iceberg adapter instance
        target_date: Optional target date for filtering
        
    Returns:
        Dictionary with row counts for each Gold table
    """
    logger.info("=" * 80)
    logger.info("GOLD LAYER: Aggregating analytics in Iceberg")
    logger.info("=" * 80)
    
    # All Gold aggregations can run in parallel
    daily_summary_count = aggregate_gold_daily_booking_summary(iceberg_adapter, target_date)
    customer_analytics_count = aggregate_gold_customer_analytics(iceberg_adapter)
    location_analytics_count = aggregate_gold_location_analytics(iceberg_adapter)
    
    results = {
        'gold.daily_booking_summary': daily_summary_count,
        'gold.customer_analytics': customer_analytics_count,
        'gold.location_analytics': location_analytics_count,
    }
    
    logger.info(f"Gold aggregation completed: {sum(results.values())} total rows")
    return results


@flow(
    name="ride-booking-granular-medallion-etl",
    description="Granular ETL pipeline implementing medallion architecture with individual entity tasks",
    log_prints=True,
)
def granular_ride_booking_etl(
    source_file: Optional[str] = None,
    extraction_date: Optional[datetime] = None,
    run_bronze: bool = True,
    run_silver: bool = True,
    run_gold: bool = True,
) -> dict[str, dict[str, int]]:
    """Main granular ETL flow orchestrating Bronze -> Silver -> Gold transformations.
    
    This flow uses individual tasks for each entity/table, enabling:
    - Fine-grained monitoring and logging
    - Parallel execution within layers
    - Easier debugging and reprocessing
    - Entity-level retries
    
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
    
    logger.info(f"Starting GRANULAR ETL pipeline for {source_path.name}")
    logger.info(f"Extraction date: {extraction_date.date()}, Month: {extraction_month}")
    logger.info(f"Iceberg warehouse: {settings.iceberg.warehouse_path}")
    
    # Initialize Iceberg adapter
    iceberg_adapter = IcebergAdapter(settings.iceberg)
    
    # Initialize Iceberg namespaces
    initialize_iceberg_namespaces(iceberg_adapter)
    
    results = {}
    
    # Bronze Layer: Extract raw data with granular tasks
    if run_bronze:
        bronze_results = bronze_extraction_flow(
            source_file=source_path,
            iceberg_adapter=iceberg_adapter,
            extraction_date=extraction_date,
        )
        results['bronze'] = bronze_results
    
    # Silver Layer: Transform to normalized model with granular tasks
    if run_silver:
        silver_results = silver_transformation_flow(
            iceberg_adapter=iceberg_adapter,
            extraction_month=extraction_month if run_bronze else None,
        )
        results['silver'] = silver_results
    
    # Gold Layer: Aggregate analytics with granular tasks
    if run_gold:
        gold_results = gold_aggregation_flow(
            iceberg_adapter=iceberg_adapter,
            target_date=extraction_date if run_bronze else None,
        )
        results['gold'] = gold_results
    
    logger.info("=" * 80)
    logger.info("GRANULAR ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"Total results: {results}")
    logger.info(f"Data stored in: {settings.iceberg.warehouse_path}")
    
    return results


# Keep backward compatibility with existing flow names
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
    """Main ETL flow - delegates to granular implementation.
    
    This is kept for backward compatibility.
    """
    return granular_ride_booking_etl(
        source_file=source_file,
        extraction_date=extraction_date,
        run_bronze=run_bronze,
        run_silver=run_silver,
        run_gold=run_gold,
    )


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
    return granular_ride_booking_etl(
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
    return granular_ride_booking_etl(
        source_file=None,
        extraction_date=None,
        run_bronze=False,  # Skip Bronze, use existing data
        run_silver=True,
        run_gold=True,
    )


if __name__ == "__main__":
    # Allow running the flow directly from command line
    granular_ride_booking_etl()

#!/usr/bin/env python
"""Enhanced CLI for running granular ride booking ETL pipelines."""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from app.etl.flows import (
    ride_booking_etl,
    bronze_extraction_flow,
    silver_transformation_flow,
    gold_aggregation_flow,
    incremental_etl,
    backfill_etl,
)
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run granular ride booking ETL pipeline with medallion architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete ETL pipeline
  python -m app.etl.cli run --source-file data/ncr_ride_bookings.csv
  
  # Run only Bronze layer
  python -m app.etl.cli bronze --source-file data/ncr_ride_bookings.csv
  
  # Run only Silver layer
  python -m app.etl.cli silver
  
  # Run only Gold layer
  python -m app.etl.cli gold
  
  # Run incremental ETL
  python -m app.etl.cli incremental --source-file data/new_bookings.csv
  
  # Run backfill (Silver + Gold only)
  python -m app.etl.cli backfill
        """
    )
    
    parser.add_argument(
        "command",
        choices=["run", "bronze", "silver", "gold", "incremental", "backfill"],
        help="ETL command to execute",
    )
    
    parser.add_argument(
        "--source-file",
        "-s",
        type=str,
        help="Path to source CSV file (defaults to data/ncr_ride_bookings.csv)",
    )
    
    parser.add_argument(
        "--extraction-date",
        "-d",
        type=str,
        help="Extraction date in YYYY-MM-DD format (defaults to today)",
    )
    
    parser.add_argument(
        "--extraction-month",
        "-m",
        type=str,
        help="Extraction month in YYYY-MM format (for Silver layer filtering)",
    )
    
    parser.add_argument(
        "--warehouse",
        "-w",
        type=str,
        help="Path to Iceberg warehouse directory (defaults to ./warehouse)",
    )
    
    parser.add_argument(
        "--no-date-filter",
        action="store_true",
        help="Disable date filtering in Bronze layer (process all data regardless of date)",
    )
    
    return parser.parse_args()


def print_summary(results: dict[str, dict[str, int]]):
    """Print execution summary."""
    print("\n" + "=" * 80)
    print("ETL EXECUTION SUMMARY")
    print("=" * 80)
    for layer, counts in results.items():
        print(f"\n{layer.upper()} Layer:")
        for table, count in counts.items():
            print(f"  {table}: {count:,} rows")
    print("\n" + "=" * 80)


def main():
    """Main entry point for granular ETL CLI."""
    args = parse_args()
    
    # Parse extraction date if provided
    extraction_date = None
    if args.extraction_date:
        try:
            extraction_date = datetime.strptime(args.extraction_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format: {args.extraction_date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Validate source file if provided
    source_file = args.source_file
    if source_file and not Path(source_file).exists():
        logger.error(f"Source file not found: {source_file}")
        sys.exit(1)
    
    # Load settings
    settings = load_settings()
    warehouse_path = Path(args.warehouse) if args.warehouse else settings.iceberg.warehouse_path
    
    try:
        if args.command == "run":
            logger.info("Running complete granular ETL pipeline...")
            results = ride_booking_etl(
                source_file=source_file,
                extraction_date=extraction_date,
                run_bronze=True,
                run_silver=True,
                run_gold=True,
                no_date_filter=args.no_date_filter,
            )
        
        elif args.command == "bronze":
            if not source_file:
                logger.error("--source-file is required for Bronze layer extraction")
                sys.exit(1)
            
            logger.info("Running Bronze layer extraction...")
            iceberg_adapter = IcebergAdapter(settings.iceberg)
            iceberg_adapter.create_namespace('bronze')
            
            bronze_results = bronze_extraction_flow(
                source_file=Path(source_file),
                iceberg_adapter=iceberg_adapter,
                extraction_date=extraction_date,
                no_date_filter=args.no_date_filter,
            )
            results = {'bronze': bronze_results}
        
        elif args.command == "silver":
            logger.info("Running Silver layer transformation...")
            iceberg_adapter = IcebergAdapter(settings.iceberg)
            iceberg_adapter.create_namespace('silver')
            
            silver_results = silver_transformation_flow(
                iceberg_adapter=iceberg_adapter,
                extraction_month=args.extraction_month,
            )
            results = {'silver': silver_results}
        
        elif args.command == "gold":
            logger.info("Running Gold layer aggregation...")
            iceberg_adapter = IcebergAdapter(settings.iceberg)
            iceberg_adapter.create_namespace('gold')
            
            gold_results = gold_aggregation_flow(
                iceberg_adapter=iceberg_adapter,
                target_date=extraction_date,
            )
            results = {'gold': gold_results}
        
        elif args.command == "incremental":
            if not source_file:
                logger.error("--source-file is required for incremental ETL")
                sys.exit(1)
            
            logger.info("Running incremental ETL...")
            results = incremental_etl(
                source_file=source_file,
                extraction_date=extraction_date,
            )
        
        elif args.command == "backfill":
            logger.info("Running backfill ETL (Silver + Gold)...")
            results = backfill_etl()
        
        else:
            logger.error(f"Unknown command: {args.command}")
            sys.exit(1)
        
        # Print summary
        print_summary(results)
        
        logger.info("ETL pipeline completed successfully!")
        logger.info(f"Data stored in: {settings.iceberg.warehouse_path}")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

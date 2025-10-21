#!/usr/bin/env python
"""Command-line interface for running ride booking ETL pipelines."""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from app.etl.flows import backfill_etl, incremental_etl, ride_booking_etl

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
        description="Run ride booking ETL pipeline with medallion architecture"
    )
    
    parser.add_argument(
        "command",
        choices=["run", "incremental", "backfill"],
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
        "--skip-bronze",
        action="store_true",
        help="Skip Bronze layer extraction",
    )
    
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip Silver layer transformation",
    )
    
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip Gold layer aggregation",
    )
    
    return parser.parse_args()


def main():
    """Main entry point for ETL CLI."""
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
    
    try:
        if args.command == "run":
            logger.info("Running full ETL pipeline...")
            results = ride_booking_etl(
                source_file=source_file,
                extraction_date=extraction_date,
                run_bronze=not args.skip_bronze,
                run_silver=not args.skip_silver,
                run_gold=not args.skip_gold,
            )
        
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
            logger.info("Running backfill ETL...")
            results = backfill_etl()
        
        else:
            logger.error(f"Unknown command: {args.command}")
            sys.exit(1)
        
        # Print summary
        print("\n" + "=" * 80)
        print("ETL EXECUTION SUMMARY")
        print("=" * 80)
        for layer, counts in results.items():
            print(f"\n{layer.upper()} Layer:")
            for table, count in counts.items():
                print(f"  {table}: {count} rows")
        print("\n" + "=" * 80)
        
        logger.info("ETL pipeline completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

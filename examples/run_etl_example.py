"""Example script demonstrating how to use the ride booking ETL pipeline."""

import logging
from pathlib import Path

from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings
from app.etl.flows import ride_booking_etl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def check_database_connection():
    """Verify database connection."""
    logger.info("Checking database connection...")
    
    settings = load_settings()
    db_adapter = PostgreSQLAdapter(settings.database)
    
    try:
        with db_adapter.connection() as conn:
            result = conn.execute("SELECT version()")
            version = result.fetchone()[0]
            logger.info(f"✓ Connected to PostgreSQL: {version[:50]}...")
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False
    finally:
        db_adapter.close()


def verify_schemas():
    """Verify that required schemas exist."""
    logger.info("Verifying database schemas...")
    
    settings = load_settings()
    db_adapter = PostgreSQLAdapter(settings.database)
    
    required_schemas = ['bronze', 'silver', 'gold']
    
    try:
        query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('bronze', 'silver', 'gold')
        """
        result = db_adapter.execute_query(query)
        existing_schemas = [row['schema_name'] for row in result]
        
        for schema in required_schemas:
            if schema in existing_schemas:
                logger.info(f"✓ Schema '{schema}' exists")
            else:
                logger.warning(f"✗ Schema '{schema}' not found - will be created")
        
        return True
    except Exception as e:
        logger.error(f"Schema verification failed: {e}")
        return False
    finally:
        db_adapter.close()


def count_source_rows():
    """Count rows in source CSV file."""
    settings = load_settings()
    source_file = settings.data_path / "ncr_ride_bookings.csv"
    
    if not source_file.exists():
        logger.error(f"✗ Source file not found: {source_file}")
        return 0
    
    # Simple line count (excluding header)
    with open(source_file, 'r') as f:
        row_count = sum(1 for _ in f) - 1
    
    logger.info(f"✓ Source file has {row_count:,} rows: {source_file.name}")
    return row_count


def run_sample_etl():
    """Run a sample ETL pipeline."""
    logger.info("=" * 80)
    logger.info("RUNNING SAMPLE ETL PIPELINE")
    logger.info("=" * 80)
    
    settings = load_settings()
    source_file = str(settings.data_path / "ncr_ride_bookings.csv")
    
    try:
        # Run the ETL
        results = ride_booking_etl(
            source_file=source_file,
            run_bronze=True,
            run_silver=True,
            run_gold=True,
        )
        
        # Display results
        logger.info("=" * 80)
        logger.info("ETL PIPELINE RESULTS")
        logger.info("=" * 80)
        
        for layer, counts in results.items():
            logger.info(f"\n{layer.upper()} Layer:")
            for table, count in counts.items():
                logger.info(f"  {table}: {count:,} rows")
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ ETL pipeline completed successfully!")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ETL pipeline failed: {e}", exc_info=True)
        return False


def query_sample_data():
    """Query some sample data from each layer."""
    logger.info("=" * 80)
    logger.info("QUERYING SAMPLE DATA")
    logger.info("=" * 80)
    
    settings = load_settings()
    db_adapter = PostgreSQLAdapter(settings.database)
    
    try:
        # Bronze layer sample
        logger.info("\nBronze Layer - Sample Booking:")
        bronze_query = "SELECT * FROM bronze.booking ORDER BY created_at DESC LIMIT 1"
        bronze_result = db_adapter.execute_query(bronze_query)
        if bronze_result:
            for key, value in bronze_result[0].items():
                logger.info(f"  {key}: {value}")
        
        # Silver layer sample
        logger.info("\nSilver Layer - Booking with Dimensions:")
        silver_query = """
            SELECT 
                b.booking_id,
                b.date,
                b.booking_value,
                c.customer_id,
                vt.name as vehicle_type,
                bs.name as status
            FROM silver.booking b
            LEFT JOIN silver.customer c ON b.customer_id = c.customer_id
            LEFT JOIN silver.vehicle_type vt ON b.vehicle_type_id = vt.vehicle_type_id
            LEFT JOIN silver.booking_status bs ON b.booking_status_id = bs.booking_status_id
            ORDER BY b.created_at DESC
            LIMIT 1
        """
        silver_result = db_adapter.execute_query(silver_query)
        if silver_result:
            for key, value in silver_result[0].items():
                logger.info(f"  {key}: {value}")
        
        # Gold layer sample
        logger.info("\nGold Layer - Daily Summary Statistics:")
        gold_query = """
            SELECT 
                summary_date,
                total_bookings,
                completed_rides,
                cancelled_rides,
                total_revenue,
                ROUND(avg_ride_distance::numeric, 2) as avg_distance,
                ROUND(avg_driver_rating::numeric, 2) as avg_driver_rating
            FROM gold.daily_booking_summary
            ORDER BY summary_date DESC
            LIMIT 5
        """
        gold_result = db_adapter.execute_query(gold_query)
        if gold_result:
            logger.info(f"  Found {len(gold_result)} days of data")
            for row in gold_result:
                logger.info(f"  {row['summary_date']}: {row['total_bookings']} bookings, "
                          f"${row['total_revenue']:.2f} revenue")
        
        logger.info("\n" + "=" * 80)
        return True
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return False
    finally:
        db_adapter.close()


def main():
    """Main example workflow."""
    logger.info("=" * 80)
    logger.info("RIDE BOOKING ETL - EXAMPLE SCRIPT")
    logger.info("=" * 80)
    
    # Step 1: Check database connection
    if not check_database_connection():
        logger.error("Cannot proceed without database connection")
        return
    
    # Step 2: Verify schemas
    verify_schemas()
    
    # Step 3: Count source rows
    row_count = count_source_rows()
    if row_count == 0:
        logger.error("Cannot proceed without source data")
        return
    
    # Step 4: Run ETL pipeline
    if not run_sample_etl():
        logger.error("ETL pipeline failed")
        return
    
    # Step 5: Query sample data
    query_sample_data()
    
    logger.info("=" * 80)
    logger.info("EXAMPLE SCRIPT COMPLETED")
    logger.info("=" * 80)
    logger.info("\nNext steps:")
    logger.info("  1. Explore data in Bronze/Silver/Gold schemas")
    logger.info("  2. Run: docker-compose --profile dev up -d pgadmin")
    logger.info("  3. Access pgAdmin at http://localhost:5050")
    logger.info("  4. Use CLI: python -m app.etl.cli --help")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

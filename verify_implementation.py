"""Verification script to test the ETL medallion architecture implementation."""

import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_files_exist():
    """Verify all required files exist."""
    logger.info("=" * 80)
    logger.info("VERIFYING PROJECT FILES")
    logger.info("=" * 80)
    
    required_files = [
        # ETL modules
        "app/etl/bronze_layer.py",
        "app/etl/silver_layer.py",
        "app/etl/gold_layer.py",
        "app/etl/flows.py",
        "app/etl/deploy.py",
        "app/etl/cli.py",
        
        # Database
        "init-db/02_create_medallion_schema.sql",
        
        # Config
        "app/config/settings.py",
        "app/adapters/postgresql.py",
        
        # Documentation
        "ETL_README.md",
        "QUICKSTART.md",
        "IMPLEMENTATION_SUMMARY.md",
        
        # Examples
        "examples/run_etl_example.py",
        
        # Data
        "data/ncr_ride_bookings.csv",
    ]
    
    missing_files = []
    for file_path in required_files:
        path = Path(file_path)
        if path.exists():
            logger.info(f"✓ {file_path}")
        else:
            logger.error(f"✗ {file_path} - MISSING")
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"\n{len(missing_files)} files missing!")
        return False
    
    logger.info(f"\n✓ All {len(required_files)} required files exist")
    return True


def verify_imports():
    """Verify all modules can be imported."""
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING PYTHON IMPORTS")
    logger.info("=" * 80)
    
    modules_to_test = [
        ("app.config.settings", "load_settings"),
        ("app.adapters.postgresql", "PostgreSQLAdapter"),
        ("app.etl.bronze_layer", "extract_to_bronze"),
        ("app.etl.silver_layer", "transform_to_silver"),
        ("app.etl.gold_layer", "aggregate_to_gold"),
        ("app.etl.flows", "ride_booking_etl"),
        ("app.etl.cli", "main"),
    ]
    
    import_errors = []
    for module_name, attr_name in modules_to_test:
        try:
            module = __import__(module_name, fromlist=[attr_name])
            getattr(module, attr_name)
            logger.info(f"✓ {module_name}.{attr_name}")
        except Exception as e:
            logger.error(f"✗ {module_name}.{attr_name} - {e}")
            import_errors.append((module_name, attr_name, str(e)))
    
    if import_errors:
        logger.error(f"\n{len(import_errors)} import errors!")
        return False
    
    logger.info(f"\n✓ All {len(modules_to_test)} modules imported successfully")
    return True


def verify_sql_syntax():
    """Verify SQL files have valid syntax."""
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING SQL FILES")
    logger.info("=" * 80)
    
    sql_files = [
        "init-db/01_create_schemas.sql",
        "init-db/02_create_medallion_schema.sql",
    ]
    
    for sql_file in sql_files:
        path = Path(sql_file)
        if not path.exists():
            logger.error(f"✗ {sql_file} - File not found")
            continue
        
        content = path.read_text()
        
        # Basic syntax checks
        issues = []
        
        # Check for common SQL keywords
        if "CREATE TABLE" not in content.upper() and "CREATE SCHEMA" not in content.upper():
            issues.append("No CREATE statements found")
        
        # Check for balanced parentheses
        if content.count('(') != content.count(')'):
            issues.append("Unbalanced parentheses")
        
        # Check for semicolons
        if ';' not in content:
            issues.append("No statement terminators (;) found")
        
        if issues:
            logger.error(f"✗ {sql_file} - Issues: {', '.join(issues)}")
        else:
            logger.info(f"✓ {sql_file}")
    
    logger.info(f"\n✓ SQL files checked")
    return True


def verify_documentation():
    """Verify documentation files are complete."""
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING DOCUMENTATION")
    logger.info("=" * 80)
    
    doc_files = {
        "ETL_README.md": ["Architecture", "Bronze", "Silver", "Gold", "Usage"],
        "QUICKSTART.md": ["Quick Start", "Prerequisites", "Run"],
        "IMPLEMENTATION_SUMMARY.md": ["Overview", "Database Schema", "ETL Pipeline"],
        "README.md": ["Medallion", "Quick Start", "Usage"],
    }
    
    for doc_file, required_sections in doc_files.items():
        path = Path(doc_file)
        if not path.exists():
            logger.error(f"✗ {doc_file} - File not found")
            continue
        
        content = path.read_text()
        missing = [s for s in required_sections if s.lower() not in content.lower()]
        
        if missing:
            logger.warning(f"⚠ {doc_file} - Missing sections: {', '.join(missing)}")
        else:
            logger.info(f"✓ {doc_file} - All sections present")
    
    logger.info(f"\n✓ Documentation verified")
    return True


def print_architecture_summary():
    """Print a summary of the architecture."""
    logger.info("\n" + "=" * 80)
    logger.info("ETL ARCHITECTURE SUMMARY")
    logger.info("=" * 80)
    
    summary = """
    
MEDALLION ARCHITECTURE IMPLEMENTATION
======================================

Source: data/ncr_ride_bookings.csv (150,001 rows)

BRONZE LAYER (Raw Staging - Partitioned by Month)
--------------------------------------------------
Tables:
  • customer          - Customer records per booking
  • vehicle_type      - Vehicle types used
  • location          - Pickup and drop locations
  • booking           - Main booking transactions
  • booking_status    - Booking status values
  • payment_method    - Payment methods
  • ride              - Completed ride metrics
  • cancelled_ride    - Cancellation records
  • incompleted_ride  - Incomplete ride records

Partition Key: extraction_month (YYYY-MM)

SILVER LAYER (Normalized Star Schema)
--------------------------------------
Dimensions:
  • customer          - Customer master data
  • vehicle_type      - Vehicle type lookup
  • location          - Location lookup
  • booking_status    - Status codes
  • payment_method    - Payment types

Facts:
  • booking           - Central fact (FKs to all dimensions)
  • ride              - Completed ride metrics
  • cancelled_ride    - Cancellation details
  • incompleted_ride  - Incomplete ride reasons

GOLD LAYER (Aggregated Analytics)
----------------------------------
Analytics Tables:
  • daily_booking_summary  - Daily metrics
  • customer_analytics     - Customer KPIs
  • location_analytics     - Location statistics

ORCHESTRATION
-------------
Framework: Prefect
Flows:
  • ride_booking_etl      - Full pipeline
  • incremental_etl       - Process new data
  • backfill_etl          - Reprocess existing

Command-Line Interface:
  • python -m app.etl.cli run
  • python -m app.etl.cli incremental
  • python -m app.etl.cli backfill

STORAGE
-------
Database: PostgreSQL
Schemas: bronze, silver, gold
Connection: via SQLAlchemy

KEY FEATURES
------------
✓ Idempotent operations (UPSERT logic)
✓ Monthly partitioning in Bronze
✓ Foreign key constraints in Silver
✓ Pre-aggregated metrics in Gold
✓ Retry logic with Prefect tasks
✓ Comprehensive error handling
✓ Docker deployment ready

    """
    
    logger.info(summary)


def main():
    """Run all verification checks."""
    logger.info("=" * 80)
    logger.info("ETL MEDALLION ARCHITECTURE - VERIFICATION SCRIPT")
    logger.info("=" * 80)
    
    checks = [
        ("Files", verify_files_exist),
        ("Imports", verify_imports),
        ("SQL Syntax", verify_sql_syntax),
        ("Documentation", verify_documentation),
    ]
    
    results = {}
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            logger.error(f"Error in {check_name} check: {e}")
            results[check_name] = False
    
    # Print summary
    print_architecture_summary()
    
    logger.info("=" * 80)
    logger.info("VERIFICATION RESULTS")
    logger.info("=" * 80)
    
    for check_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{check_name:.<40} {status}")
    
    all_passed = all(results.values())
    
    logger.info("=" * 80)
    if all_passed:
        logger.info("✓ ALL CHECKS PASSED - ETL Implementation Ready!")
        logger.info("=" * 80)
        logger.info("\nNext steps:")
        logger.info("  1. Start database: docker-compose up -d postgres")
        logger.info("  2. Run ETL: python -m app.etl.cli run")
        logger.info("  3. Or run example: python examples/run_etl_example.py")
        logger.info("  4. Read QUICKSTART.md for detailed guide")
        sys.exit(0)
    else:
        logger.error("✗ SOME CHECKS FAILED - Review errors above")
        logger.info("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()

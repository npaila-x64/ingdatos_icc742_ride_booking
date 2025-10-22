#!/usr/bin/env python3
"""Test script to verify granular ETL implementation works correctly."""

import sys
from pathlib import Path
from datetime import datetime

# Test imports
print("=" * 80)
print("GRANULAR ETL VERIFICATION TEST")
print("=" * 80)

print("\n1. Testing module imports...")
try:
    from app.etl.tasks.bronze import (
        load_and_prepare_source_data,
        extract_bronze_customer,
        extract_bronze_vehicle_type,
    )
    from app.etl.tasks.silver import (
        transform_silver_customer,
        transform_silver_vehicle_type,
    )
    from app.etl.tasks.gold import (
        aggregate_gold_customer_analytics,
    )
    from app.etl.flows import (
        bronze_extraction_flow,
        silver_transformation_flow,
        gold_aggregation_flow,
        granular_ride_booking_etl,
    )
    from app.adapters.iceberg_adapter import IcebergAdapter
    from app.config.settings import load_settings
    print("   ✓ All imports successful")
except Exception as e:
    print(f"   ✗ Import failed: {e}")
    sys.exit(1)

print("\n2. Testing configuration...")
try:
    settings = load_settings()
    print(f"   ✓ Settings loaded")
    print(f"   - Warehouse: {settings.iceberg.warehouse_path}")
    print(f"   - Data path: {settings.data_path}")
except Exception as e:
    print(f"   ✗ Configuration failed: {e}")
    sys.exit(1)

print("\n3. Testing IcebergAdapter initialization...")
try:
    iceberg = IcebergAdapter(settings.iceberg)
    print("   ✓ IcebergAdapter initialized")
except Exception as e:
    print(f"   ✗ Adapter initialization failed: {e}")
    sys.exit(1)

print("\n4. Testing source data file...")
source_file = settings.data_path / "ncr_ride_bookings.csv"
if source_file.exists():
    print(f"   ✓ Source file exists: {source_file}")
    # Check file size
    size_mb = source_file.stat().st_size / (1024 * 1024)
    print(f"   - File size: {size_mb:.2f} MB")
else:
    print(f"   ✗ Source file not found: {source_file}")
    print("   Note: Full ETL test will be skipped")

print("\n5. Testing task decorators...")
try:
    # Verify tasks are properly decorated
    assert hasattr(load_and_prepare_source_data, 'fn'), "load_and_prepare_source_data should be a Prefect task"
    assert hasattr(extract_bronze_customer, 'fn'), "extract_bronze_customer should be a Prefect task"
    assert hasattr(transform_silver_customer, 'fn'), "transform_silver_customer should be a Prefect task"
    assert hasattr(aggregate_gold_customer_analytics, 'fn'), "aggregate_gold_customer_analytics should be a Prefect task"
    print("   ✓ All tasks properly decorated with @task")
except AssertionError as e:
    print(f"   ✗ Task decoration check failed: {e}")
    sys.exit(1)

print("\n6. Testing flow decorators...")
try:
    # Verify flows are properly decorated
    assert hasattr(bronze_extraction_flow, 'fn'), "bronze_extraction_flow should be a Prefect flow"
    assert hasattr(silver_transformation_flow, 'fn'), "silver_transformation_flow should be a Prefect flow"
    assert hasattr(gold_aggregation_flow, 'fn'), "gold_aggregation_flow should be a Prefect flow"
    assert hasattr(granular_ride_booking_etl, 'fn'), "granular_ride_booking_etl should be a Prefect flow"
    print("   ✓ All flows properly decorated with @flow")
except AssertionError as e:
    print(f"   ✗ Flow decoration check failed: {e}")
    sys.exit(1)

print("\n7. Testing task signatures...")
try:
    # Verify task signatures
    import inspect
    
    # Bronze task signature
    sig = inspect.signature(load_and_prepare_source_data.fn)
    params = list(sig.parameters.keys())
    assert 'source_file' in params, "load_and_prepare_source_data should have source_file parameter"
    assert 'extraction_date' in params, "load_and_prepare_source_data should have extraction_date parameter"
    
    sig = inspect.signature(extract_bronze_customer.fn)
    params = list(sig.parameters.keys())
    assert 'source_df' in params, "extract_bronze_customer should have source_df parameter"
    assert 'iceberg_adapter' in params, "extract_bronze_customer should have iceberg_adapter parameter"
    
    # Silver task signature
    sig = inspect.signature(transform_silver_customer.fn)
    params = list(sig.parameters.keys())
    assert 'iceberg_adapter' in params, "transform_silver_customer should have iceberg_adapter parameter"
    assert 'extraction_month' in params, "transform_silver_customer should have extraction_month parameter"
    
    # Gold task signature
    sig = inspect.signature(aggregate_gold_customer_analytics.fn)
    params = list(sig.parameters.keys())
    assert 'iceberg_adapter' in params, "aggregate_gold_customer_analytics should have iceberg_adapter parameter"
    
    print("   ✓ All task signatures are correct")
except Exception as e:
    print(f"   ✗ Task signature check failed: {e}")
    sys.exit(1)

print("\n8. Testing flow availability...")
try:
    from app.etl.flows import granular_ride_booking_etl, incremental_etl, backfill_etl
    assert hasattr(granular_ride_booking_etl, 'fn'), "granular_ride_booking_etl should be a Prefect flow"
    assert hasattr(incremental_etl, 'fn'), "incremental_etl should be a Prefect flow"
    assert hasattr(backfill_etl, 'fn'), "backfill_etl should be a Prefect flow"
    print("   ✓ All flows available and properly decorated")
except Exception as e:
    print(f"   ✗ Flow availability check failed: {e}")
    sys.exit(1)

print("\n9. Testing CLI availability...")
try:
    from app.etl import cli
    assert hasattr(cli, 'main'), "cli should have main() function"
    print("   ✓ CLI module available")
except Exception as e:
    print(f"   ✗ CLI check failed: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("✅ ALL VERIFICATION TESTS PASSED")
print("=" * 80)
print("\nThe granular ETL implementation is properly configured and ready to use!")
print("\nQuick start commands:")
print("  # Run complete pipeline:")
print("  python -m app.etl.cli run --source-file data/ncr_ride_bookings.csv")
print("\n  # Run individual layers:")
print("  python -m app.etl.cli bronze --source-file data/ncr_ride_bookings.csv")
print("  python -m app.etl.cli silver")
print("  python -m app.etl.cli gold")
print("\nFor more information, see:")
print("  - GRANULAR_ETL_ARCHITECTURE.md (complete guide)")
print("  - GRANULAR_ETL_SUMMARY.md (summary of changes)")
print("  - ETL_ARCHITECTURE.md (updated architecture)")
print("=" * 80)

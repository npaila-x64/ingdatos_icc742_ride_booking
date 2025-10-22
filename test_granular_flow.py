#!/usr/bin/env python3
"""Quick dry-run test of granular ETL flow orchestration."""

import sys
from pathlib import Path
from datetime import datetime

print("=" * 80)
print("GRANULAR ETL FLOW ORCHESTRATION TEST")
print("=" * 80)

try:
    from app.etl.flows import granular_ride_booking_etl
    from app.config.settings import load_settings
    
    settings = load_settings()
    source_file = str(settings.data_path / "ncr_ride_bookings.csv")
    
    # Check if source file exists
    if not Path(source_file).exists():
        print(f"\n⚠ Source file not found: {source_file}")
        print("Skipping execution test")
        sys.exit(0)
    
    print(f"\n📊 Source file: {source_file}")
    print(f"📦 Warehouse: {settings.iceberg.warehouse_path}")
    print(f"📅 Extraction date: 2024-12-01")
    
    print("\n" + "=" * 80)
    print("Starting granular ETL execution...")
    print("=" * 80)
    print("\nThis will execute:")
    print("  • Bronze Layer: 10 tasks (1 preprocessing + 9 extractions)")
    print("  • Silver Layer: 9 tasks (5 dimensions + 4 facts)")
    print("  • Gold Layer: 3 tasks (3 analytics aggregations)")
    print("  Total: 22 individual orchestrated tasks\n")
    
    # Run the granular ETL
    results = granular_ride_booking_etl(
        source_file=source_file,
        extraction_date=datetime(2024, 12, 1),
        run_bronze=True,
        run_silver=True,
        run_gold=True,
    )
    
    print("\n" + "=" * 80)
    print("✅ GRANULAR ETL EXECUTION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    
    # Print results summary
    print("\n📊 EXECUTION SUMMARY:")
    total_rows = 0
    for layer, counts in results.items():
        layer_total = sum(counts.values())
        total_rows += layer_total
        print(f"\n{layer.upper()} Layer: {layer_total:,} rows")
        for table, count in sorted(counts.items()):
            print(f"  • {table}: {count:,} rows")
    
    print(f"\n📈 TOTAL ROWS PROCESSED: {total_rows:,}")
    print("\n" + "=" * 80)
    print("🎉 All 22 granular tasks executed successfully!")
    print("=" * 80)
    
except KeyboardInterrupt:
    print("\n\n⚠ Execution interrupted by user")
    sys.exit(1)
except Exception as e:
    print(f"\n\n❌ Execution failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

#!/usr/bin/env python3
"""
Helper script to create new date slice scripts from templates.

Usage:
    python scripts/create_date_slice.py --month 6 --year 2024
    
This will create:
    - scripts/date_slices/run_june_2024_etl.py
    - scripts/date_slices/verify_june_2024.py
"""

import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional


def get_month_name(month: int) -> str:
    """Get month name from month number."""
    return datetime(2000, month, 1).strftime('%B').lower()


def create_etl_script(year: int, month: int) -> Optional[Path]:
    """Create ETL script from template."""
    month_name = get_month_name(month)
    filename = f"run_{month_name}_{year}_etl.py"
    filepath = Path(__file__).parent / "date_slices" / filename
    
    template = Path(__file__).parent / "date_slices" / "_template_etl.py"
    
    if not template.exists():
        print(f"❌ Template not found: {template}")
        return None
    
    # Read template
    content = template.read_text()
    
    # Replace placeholders
    content = content.replace("YEAR = 2024", f"YEAR = {year}")
    content = content.replace("MONTH = 1  # 1-12", f"MONTH = {month}  # 1-12")
    
    # Write new file
    filepath.write_text(content)
    print(f"✅ Created ETL script: {filepath}")
    return filepath


def create_verify_script(year: int, month: int) -> Optional[Path]:
    """Create verification script from template."""
    month_name = get_month_name(month)
    filename = f"verify_{month_name}_{year}.py"
    filepath = Path(__file__).parent / "date_slices" / filename
    
    template = Path(__file__).parent / "date_slices" / "_template_verify.py"
    
    if not template.exists():
        print(f"❌ Template not found: {template}")
        return None
    
    # Read template
    content = template.read_text()
    
    # Replace placeholders
    content = content.replace("YEAR = 2024", f"YEAR = {year}")
    content = content.replace("MONTH = 1  # 1-12", f"MONTH = {month}  # 1-12")
    
    # Write new file
    filepath.write_text(content)
    print(f"✅ Created verification script: {filepath}")
    return filepath


def main():
    parser = argparse.ArgumentParser(
        description='Create new date slice scripts from templates'
    )
    parser.add_argument(
        '--year', 
        type=int, 
        required=True,
        help='Year (e.g., 2024)'
    )
    parser.add_argument(
        '--month', 
        type=int, 
        required=True,
        help='Month (1-12)'
    )
    parser.add_argument(
        '--skip-verify',
        action='store_true',
        help='Skip creating verification script'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not (1 <= args.month <= 12):
        print("❌ Error: Month must be between 1 and 12")
        return
    
    month_name = get_month_name(args.month)
    month_name_cap = month_name.capitalize()
    
    print("=" * 80)
    print(f"Creating Date Slice Scripts for {month_name_cap} {args.year}")
    print("=" * 80)
    print()
    
    # Create ETL script
    etl_script = create_etl_script(args.year, args.month)
    
    # Create verification script (unless skipped)
    verify_script = None
    if not args.skip_verify:
        verify_script = create_verify_script(args.year, args.month)
    
    print()
    print("=" * 80)
    print("Scripts Created Successfully!")
    print("=" * 80)
    print()
    print("Next steps:")
    print()
    print(f"1. Run the ETL for {month_name_cap} {args.year}:")
    if etl_script:
        print(f"   python {etl_script}")
    print()
    if verify_script:
        print(f"2. Verify the results:")
        print(f"   python {verify_script}")
        print()
    print("Or use the generic script:")
    print(f"   python scripts/run_etl_for_date.py --year {args.year} --month {args.month}")
    print()


if __name__ == "__main__":
    main()

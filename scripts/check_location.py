#!/usr/bin/env python3
"""Quick check of location table structure."""

import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings

settings = load_settings()
iceberg = IcebergAdapter(settings.iceberg)

# Check location table
location = iceberg.read_table('silver', 'location')
print("Location table columns:", location.columns.tolist())
print("Location table shape:", location.shape)
print("\nFirst few rows:")
print(location.head())

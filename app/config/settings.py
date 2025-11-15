"""Centralized project settings and utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IcebergSettings:
    """Apache Iceberg configuration with filesystem catalog."""

    warehouse_path: Path

    @classmethod
    def from_env(cls, base_path: Path) -> "IcebergSettings":
        """Create settings from environment variables."""
        warehouse_dir = os.getenv("ICEBERG_WAREHOUSE", "warehouse")
        warehouse_path = (base_path / warehouse_dir).resolve()
        return cls(warehouse_path=warehouse_path)


@dataclass(frozen=True)
class ProjectSettings:
    """Container for high-level project configuration."""

    base_path: Path
    data_path: Path
    iceberg: IcebergSettings


def load_settings() -> ProjectSettings:
    """Bind environment variables into strongly typed settings."""

    base_path = Path(os.getenv("PROJECT_BASE_PATH", Path.cwd())).resolve()
    data_path = (base_path / os.getenv("PROJECT_DATA_DIR", "data")).resolve()

    iceberg_settings = IcebergSettings.from_env(base_path)

    return ProjectSettings(
        base_path=base_path,
        data_path=data_path,
        iceberg=iceberg_settings,
    )

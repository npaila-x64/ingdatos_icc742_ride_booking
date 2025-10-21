"""Centralized project settings and utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class DatabaseSettings:
    """PostgreSQL database configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "ride_booking"
    user: str = "postgres"
    password: str = ""
    schema: str = "public"

    @property
    def connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class PrefectSettings:
    """Subset of Prefect configuration values that the project relies on."""

    profile_name: str = "ride-booking-local"
    api_url: Optional[str] = None
    storage_block: str = ""
    work_pool: str = ""


@dataclass(frozen=True)
class ProjectSettings:
    """Container for high-level project configuration."""

    base_path: Path
    data_path: Path
    prefect: PrefectSettings
    database: DatabaseSettings


def load_settings() -> ProjectSettings:
    """Bind environment variables into strongly typed settings."""

    base_path = Path(os.getenv("PROJECT_BASE_PATH", Path.cwd())).resolve()
    data_path = (base_path / os.getenv("PROJECT_DATA_DIR", "data")).resolve()

    prefect_settings = PrefectSettings(
        profile_name=os.getenv("PREFECT_PROFILE", PrefectSettings.profile_name),
        api_url=os.getenv("PREFECT_API_URL"),
        storage_block=os.getenv("PREFECT_STORAGE_BLOCK", ""),
        work_pool=os.getenv("PREFECT_WORK_POOL", ""),
    )

    database_settings = DatabaseSettings(
        host=os.getenv("DB_HOST", DatabaseSettings.host),
        port=int(os.getenv("DB_PORT", str(DatabaseSettings.port))),
        database=os.getenv("DB_NAME", DatabaseSettings.database),
        user=os.getenv("DB_USER", DatabaseSettings.user),
        password=os.getenv("DB_PASSWORD", ""),
        schema=os.getenv("DB_SCHEMA", DatabaseSettings.schema),
    )

    return ProjectSettings(
        base_path=base_path,
        data_path=data_path,
        prefect=prefect_settings,
        database=database_settings,
    )

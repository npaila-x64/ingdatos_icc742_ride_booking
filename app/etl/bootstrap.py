"""Helpers for preparing the Airflow-driven ETL environment."""

from __future__ import annotations

import json
from pathlib import Path

from app.config.settings import ProjectSettings, load_settings


def ensure_data_directories(project_settings: ProjectSettings) -> None:
    """Guarantee the data directory structure exists for raw and processed assets."""

    # Users frequently run bootstrap before collecting any data. Doing so should
    # quietly create the canonical directories that downstream flows expect.
    for child in ("raw", "processed", "logs"):
        (project_settings.data_path / child).mkdir(parents=True, exist_ok=True)


def write_settings_snapshot(project_settings: ProjectSettings) -> Path:
    """Persist a snapshot of resolved settings for observability and debugging."""

    snapshot_path = project_settings.base_path / "bootstrap-settings.json"
    payload = {
        "base_path": str(project_settings.base_path),
        "data_path": str(project_settings.data_path),
        "iceberg": {
            "warehouse_path": str(project_settings.iceberg.warehouse_path),
        },
    }

    snapshot_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return snapshot_path


def run_bootstrap() -> None:
    """Execute the full bootstrap sequence for a fresh environment."""

    project_settings = load_settings()
    ensure_data_directories(project_settings)
    write_settings_snapshot(project_settings)


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    run_bootstrap()

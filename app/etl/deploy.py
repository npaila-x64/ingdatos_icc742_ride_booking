"""Airflow deployment utilities.

Provides a helper that returns the path to the Airflow docker-compose file so other
scripts can trigger the orchestrator without duplicating relative paths.
"""

from __future__ import annotations

from pathlib import Path


def airflow_compose_path() -> Path:
    """Return the path to the Airflow docker-compose file."""

    return Path(__file__).resolve().parents[2] / "airflow" / "docker-compose.yaml"

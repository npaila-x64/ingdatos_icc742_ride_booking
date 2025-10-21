"""Helpers for preparing the Prefect-driven ETL environment."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from app.config.settings import PrefectSettings, ProjectSettings, load_settings


class PrefectNotInstalledError(RuntimeError):
    """Raised when Prefect is required but not installed in the environment."""


def _require_prefect() -> tuple[Any, ...]:
    """Import Prefect lazily so the project can be inspected without dependencies."""

    try:
        settings_module = importlib.import_module("prefect.settings")
        models_module = importlib.import_module("prefect.settings.models")
    except ModuleNotFoundError as exc:  # pragma: no cover - executed only without Prefect
        raise PrefectNotInstalledError(
            "Prefect is not installed. Install project dependencies to continue."
        ) from exc

    return (
        getattr(settings_module, "PREFECT_PROFILE"),
        getattr(settings_module, "load_profiles"),
        getattr(settings_module, "save_profiles"),
        getattr(models_module, "Profile"),
    )


def ensure_prefect_profile(settings: PrefectSettings) -> None:
    """Create or update a Prefect profile that captures project defaults."""

    (
        prefect_profile_setting,
        load_profiles,
        save_profiles,
        Profile,
    ) = _require_prefect()

    profiles = load_profiles()

    profile_payload = {
        "PREFECT_API_URL": settings.api_url or "",
        "PREFECT_STORAGE_BLOCK": settings.storage_block,
        "PREFECT_WORK_POOL": settings.work_pool,
    }

    profiles[settings.profile_name] = Profile(name=settings.profile_name, settings=profile_payload)
    save_profiles(profiles, active_profile=settings.profile_name)
    prefect_profile_setting.value(settings.profile_name)


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
        "prefect": {
            "profile_name": project_settings.prefect.profile_name,
            "api_url": project_settings.prefect.api_url,
            "storage_block": project_settings.prefect.storage_block,
            "work_pool": project_settings.prefect.work_pool,
        },
    }

    snapshot_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return snapshot_path


def run_bootstrap() -> None:
    """Execute the full bootstrap sequence for a fresh environment."""

    project_settings = load_settings()
    ensure_data_directories(project_settings)
    write_settings_snapshot(project_settings)

    if project_settings.prefect.profile_name:
        ensure_prefect_profile(project_settings.prefect)


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    run_bootstrap()

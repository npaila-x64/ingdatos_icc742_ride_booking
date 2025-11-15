"""Airflow DAGs for the ride booking medallion ETL."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from airflow.decorators import dag, task  # type: ignore
from airflow.models import Param  # type: ignore
from airflow.operators.python import get_current_context  # type: ignore

from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import ProjectSettings, load_settings
from app.etl.flows import (
    backfill_etl,
    bronze_extraction_flow,
    gold_aggregation_flow,
    incremental_etl,
    ride_booking_etl,
    silver_transformation_flow,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _load_settings() -> ProjectSettings:
    settings = load_settings()
    logger.info("Airflow resolved project base path to %s", settings.base_path)
    return settings


def _build_adapter(settings: ProjectSettings) -> IcebergAdapter:
    return IcebergAdapter(settings.iceberg)


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


@dag(
    dag_id="ride_booking_medallion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "source_file": Param("", type="string", description="Optional override path to CSV source"),
        "extraction_date": Param("", type="string", description="YYYY-MM-DD extraction date"),
        "run_bronze": Param(True, type="boolean", description="Run bronze extraction layer"),
        "run_silver": Param(True, type="boolean", description="Run silver transformation layer"),
        "run_gold": Param(True, type="boolean", description="Run gold aggregation layer"),
        "no_date_filter": Param(False, type="boolean", description="Process all data ignoring dates"),
    },
    tags=["ride-booking", "medallion"],
)
def ride_booking_medallion():
    settings = _load_settings()

    @task(task_id="bronze_layer")
    def run_bronze() -> dict:
        params = get_current_context()["params"]
        if not params["run_bronze"]:
            logger.info("Bronze layer skipped by DAG params")
            return {}

        source_file = params["source_file"] or str(settings.data_path / "ncr_ride_bookings.csv")
        extraction_date = _parse_date(params.get("extraction_date"))
        adapter = _build_adapter(settings)
        return bronze_extraction_flow(
            source_file=Path(source_file),
            iceberg_adapter=adapter,
            extraction_date=extraction_date,
            no_date_filter=params.get("no_date_filter", False),
        )

    @task(task_id="silver_layer")
    def run_silver(bronze_results: dict) -> dict:
        params = get_current_context()["params"]
        _ = bronze_results  # ensures dependency for Airflow graph
        if not params["run_silver"]:
            logger.info("Silver layer skipped by DAG params")
            return {}

        extraction_date = _parse_date(params.get("extraction_date"))
        extraction_month = extraction_date.strftime("%Y-%m") if extraction_date else None
        adapter = _build_adapter(settings)
        return silver_transformation_flow(
            iceberg_adapter=adapter,
            extraction_month=extraction_month,
        )

    @task(task_id="gold_layer")
    def run_gold(silver_results: dict) -> dict:
        params = get_current_context()["params"]
        _ = silver_results
        if not params["run_gold"]:
            logger.info("Gold layer skipped by DAG params")
            return {}

        extraction_date = _parse_date(params.get("extraction_date"))
        adapter = _build_adapter(settings)
        return gold_aggregation_flow(
            iceberg_adapter=adapter,
            target_date=extraction_date,
        )

    bronze_output = run_bronze()
    silver_output = run_silver(bronze_output)
    run_gold(silver_output)


@dag(
    dag_id="ride_booking_incremental",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "source_file": Param("", type="string", description="Required path to new CSV file"),
        "extraction_date": Param("", type="string", description="YYYY-MM-DD extraction date"),
    },
    tags=["ride-booking", "incremental"],
)
def ride_booking_incremental_dag():
    @task(task_id="incremental_etl")
    def run_incremental() -> dict:
        params = get_current_context()["params"]
        source_file_param = params.get("source_file")
        if not source_file_param:
            raise ValueError("source_file param is required for incremental DAG")

        extraction_date = _parse_date(params.get("extraction_date"))
        return incremental_etl(
            source_file=source_file_param,
            extraction_date=extraction_date,
        )

    run_incremental()


@dag(
    dag_id="ride_booking_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ride-booking", "backfill"],
)
def ride_booking_backfill_dag():
    @task(task_id="backfill_etl")
    def run_backfill() -> dict:
        return backfill_etl()

    run_backfill()


ride_booking_medallion()
ride_booking_incremental_dag()
ride_booking_backfill_dag()

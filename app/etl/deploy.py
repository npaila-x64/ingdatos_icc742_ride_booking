"""Prefect deployment configurations for ride booking ETL flows."""

import asyncio

from prefect import serve

from app.etl.flows import (
    backfill_etl,
    incremental_etl,
    ride_booking_etl,
)


async def create_deployments():
    """Create Prefect deployments for ETL flows."""
    
    # Main ETL deployment
    main_deployment = await ride_booking_etl.to_deployment(
        name="ride-booking-etl-main",
        description="Main ETL pipeline for ride booking data with medallion architecture",
        tags=["etl", "medallion", "ride-booking", "production"],
        version="1.0.0",
    )
    
    # Incremental ETL deployment (for scheduled runs)
    incremental_deployment = await incremental_etl.to_deployment(
        name="ride-booking-etl-incremental",
        description="Incremental ETL for processing new ride booking data files",
        tags=["etl", "incremental", "ride-booking"],
        version="1.0.0",
        # Schedule for daily runs at 2 AM
        # cron="0 2 * * *",
    )
    
    # Backfill ETL deployment (for manual runs)
    backfill_deployment = await backfill_etl.to_deployment(
        name="ride-booking-etl-backfill",
        description="Backfill ETL for reprocessing all historical ride booking data",
        tags=["etl", "backfill", "ride-booking"],
        version="1.0.0",
    )
    
    return [main_deployment, incremental_deployment, backfill_deployment]


async def main():
    """Serve all deployments."""
    deployments = await create_deployments()
    
    print("=" * 80)
    print("Starting Prefect deployment server for Ride Booking ETL")
    print("=" * 80)
    print(f"Total deployments: {len(deployments)}")
    for deployment in deployments:
        print(f"  - {deployment.name}")
    print("=" * 80)
    
    # Serve all deployments
    await serve(*deployments)


if __name__ == "__main__":
    asyncio.run(main())

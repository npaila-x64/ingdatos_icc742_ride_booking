"""Example usage of the PostgreSQL adapter."""

from __future__ import annotations

import logging

import pandas as pd
from dotenv import load_dotenv

from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def example_basic_operations():
    """Demonstrate basic database operations."""
    # Load environment variables and settings
    load_dotenv()
    settings = load_settings()

    # Initialize adapter
    adapter = PostgreSQLAdapter(settings.database)

    try:
        # Example 1: Execute a simple query
        logger.info("Example 1: Executing simple query")
        result = adapter.execute_query("SELECT current_database(), current_user")
        logger.info(f"Current database: {result[0]}")

        # Example 2: Create a schema
        logger.info("Example 2: Creating schema")
        adapter.create_schema("ride_analytics", if_not_exists=True)

        # Example 3: Check if table exists
        logger.info("Example 3: Checking if table exists")
        exists = adapter.table_exists("rides", schema="public")
        logger.info(f"Table 'rides' exists: {exists}")

        # Example 4: Write DataFrame to database
        logger.info("Example 4: Writing DataFrame to database")
        sample_data = pd.DataFrame({
            "ride_id": [1, 2, 3],
            "passenger_id": [101, 102, 103],
            "driver_id": [201, 202, 203],
            "distance_km": [5.2, 10.8, 3.5],
            "fare": [12.50, 25.00, 8.75]
        })

        # This will create the table if it doesn't exist
        adapter.write_table(
            sample_data,
            "rides_sample",
            schema="ride_analytics",
            if_exists="replace"
        )

        # Example 5: Read data from table
        logger.info("Example 5: Reading data from table")
        df = adapter.read_table(
            "rides_sample",
            schema="ride_analytics",
            columns=["ride_id", "passenger_id", "fare"],
            where="fare > :min_fare",
            params={"min_fare": 10.0}
        )
        logger.info(f"Read {len(df)} rides with fare > 10.0")
        logger.info(f"\n{df}")

        # Example 6: Execute update statement
        logger.info("Example 6: Executing update statement")
        rows_updated = adapter.execute_statement(
            "UPDATE ride_analytics.rides_sample SET fare = fare * 1.1 WHERE ride_id = :id",
            params={"id": 1}
        )
        logger.info(f"Updated {rows_updated} rows")

    except Exception as e:
        logger.error(f"Error during database operations: {e}")
        raise
    finally:
        # Clean up
        adapter.close()


def example_context_manager():
    """Demonstrate using adapter as context manager."""
    load_dotenv()
    settings = load_settings()

    # Using context manager ensures proper cleanup
    with PostgreSQLAdapter(settings.database) as adapter:
        result = adapter.execute_query("SELECT version()")
        logger.info(f"PostgreSQL version: {result[0]['version']}")


def example_etl_pattern():
    """Demonstrate a simple ETL pattern."""
    load_dotenv()
    settings = load_settings()

    with PostgreSQLAdapter(settings.database) as adapter:
        # Extract: Read from source table
        logger.info("Extract: Reading source data")
        source_df = adapter.read_table("rides_sample", schema="ride_analytics")

        # Transform: Apply business logic
        logger.info("Transform: Applying transformations")
        transformed_df = source_df.copy()
        transformed_df["fare_with_tax"] = transformed_df["fare"] * 1.15
        transformed_df["fare_category"] = pd.cut(
            transformed_df["fare"],
            bins=[0, 10, 20, float("inf")],
            labels=["low", "medium", "high"]
        )

        # Load: Write to destination table
        logger.info("Load: Writing transformed data")
        adapter.write_table(
            transformed_df,
            "rides_transformed",
            schema="ride_analytics",
            if_exists="replace"
        )

        logger.info("ETL process completed successfully")


if __name__ == "__main__":
    # Run examples
    logger.info("=" * 60)
    logger.info("Running PostgreSQL Adapter Examples")
    logger.info("=" * 60)

    try:
        example_basic_operations()
        print("\n" + "=" * 60 + "\n")
        example_context_manager()
        print("\n" + "=" * 60 + "\n")
        example_etl_pattern()
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise

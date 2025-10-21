"""PostgreSQL database adapter using SQLAlchemy."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config.settings import DatabaseSettings

logger = logging.getLogger(__name__)


class PostgreSQLAdapter:
    """Adapter for PostgreSQL database operations using SQLAlchemy."""

    def __init__(self, settings: DatabaseSettings):
        """Initialize PostgreSQL adapter with database settings.

        Args:
            settings: Database configuration settings
        """
        self.settings = settings
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance
        """
        if self._engine is None:
            self._engine = create_engine(
                self.settings.connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                echo=False,
            )
            logger.info(
                f"Created database engine for {self.settings.host}:{self.settings.port}/"
                f"{self.settings.database}"
            )
        return self._engine

    @contextmanager
    def connection(self) -> Generator:
        """Context manager for database connections.

        Yields:
            SQLAlchemy connection object

        Example:
            >>> with adapter.connection() as conn:
            ...     result = conn.execute(text("SELECT 1"))
        """
        conn = self.engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[dict] = None) -> list[dict]:
        """Execute a SQL query and return results as a list of dictionaries.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of row dictionaries

        Example:
            >>> results = adapter.execute_query(
            ...     "SELECT * FROM rides WHERE id = :ride_id",
            ...     params={"ride_id": 123}
            ... )
        """
        try:
            with self.connection() as conn:
                result = conn.execute(text(query), params or {})
                if result.returns_rows:
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in result.fetchall()]
                return []
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def execute_statement(self, statement: str, params: Optional[dict] = None) -> int:
        """Execute a SQL statement (INSERT, UPDATE, DELETE) and return rows affected.

        Args:
            statement: SQL statement string
            params: Optional statement parameters

        Returns:
            Number of rows affected

        Example:
            >>> rows = adapter.execute_statement(
            ...     "UPDATE rides SET status = :status WHERE id = :ride_id",
            ...     params={"status": "completed", "ride_id": 123}
            ... )
        """
        try:
            with self.connection() as conn:
                result = conn.execute(text(statement), params or {})
                return result.rowcount
        except SQLAlchemyError as e:
            logger.error(f"Statement execution failed: {e}")
            raise

    def read_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> pd.DataFrame:
        """Read data from a table into a pandas DataFrame.

        Args:
            table_name: Name of the table to read
            schema: Database schema (defaults to configured schema)
            columns: List of columns to select (defaults to all)
            where: Optional WHERE clause
            params: Optional parameters for WHERE clause

        Returns:
            DataFrame containing the table data

        Example:
            >>> df = adapter.read_table(
            ...     "rides",
            ...     columns=["id", "passenger_id", "created_at"],
            ...     where="created_at > :start_date",
            ...     params={"start_date": "2025-01-01"}
            ... )
        """
        schema = schema or self.settings.schema
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {schema}.{table_name}"

        if where:
            query += f" WHERE {where}"

        try:
            with self.connection() as conn:
                df = pd.read_sql(text(query), conn, params=params or {})
                logger.info(f"Read {len(df)} rows from {schema}.{table_name}")
                return df
        except SQLAlchemyError as e:
            logger.error(f"Failed to read table {schema}.{table_name}: {e}")
            raise

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",  # Literal['fail', 'replace', 'append']
        index: bool = False,
        method: Optional[str] = None,  # Literal['multi'] or callable
        chunksize: Optional[int] = None,
    ) -> int:
        """Write a pandas DataFrame to a database table.

        Args:
            df: DataFrame to write
            table_name: Name of the target table
            schema: Database schema (defaults to configured schema)
            if_exists: How to behave if table exists: 'fail', 'replace', or 'append'
            index: Whether to write DataFrame index
            method: Insert method: None, 'multi', or callable
            chunksize: Number of rows to write at a time

        Returns:
            Number of rows written

        Example:
            >>> df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> adapter.write_table(df, "users", if_exists="append")
        """
        schema = schema or self.settings.schema

        try:
            with self.connection() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists=if_exists,
                    index=index,
                    method=method,
                    chunksize=chunksize,
                )
                logger.info(f"Wrote {len(df)} rows to {schema}.{table_name}")
                return len(df)
        except SQLAlchemyError as e:
            logger.error(f"Failed to write to table {schema}.{table_name}: {e}")
            raise

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Name of the table
            schema: Database schema (defaults to configured schema)

        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.settings.schema
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :schema
                AND table_name = :table_name
            )
        """
        result = self.execute_query(query, {"schema": schema, "table_name": table_name})
        return result[0]["exists"] if result else False

    def create_schema(self, schema_name: str, if_not_exists: bool = True) -> None:
        """Create a database schema.

        Args:
            schema_name: Name of the schema to create
            if_not_exists: Only create if schema doesn't exist

        Example:
            >>> adapter.create_schema("analytics")
        """
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        statement = f"CREATE SCHEMA {if_not_exists_clause}{schema_name}"
        self.execute_statement(statement)
        logger.info(f"Created schema: {schema_name}")

    def close(self) -> None:
        """Close database engine and cleanup resources."""
        if self._engine is not None:
            self._engine.dispose()
            logger.info("Database engine disposed")
            self._engine = None

    def __enter__(self) -> PostgreSQLAdapter:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit with cleanup."""
        self.close()

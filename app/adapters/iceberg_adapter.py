"""Apache Iceberg adapter using PyIceberg with filesystem catalog."""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from app.config.settings import IcebergSettings

logger = logging.getLogger(__name__)


class IcebergAdapter:
    """Adapter for Apache Iceberg operations using PyIceberg with filesystem catalog."""

    def __init__(self, settings: IcebergSettings):
        """Initialize Iceberg adapter with settings.

        Args:
            settings: Iceberg configuration settings
        """
        self.settings = settings
        self._catalog = None
        
        # Ensure warehouse directory exists
        self.settings.warehouse_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Iceberg warehouse path: {self.settings.warehouse_path}")

    @property
    def catalog(self):
        """Get or create Iceberg catalog.

        Returns:
            PyIceberg catalog instance
        """
        if self._catalog is None:
            # Use filesystem catalog (similar to Hadoop) for local storage
            self._catalog = load_catalog(
                "default",
                **{
                    "type": "sql",
                    "uri": f"sqlite:///{self.settings.warehouse_path}/catalog.db",
                    "warehouse": str(self.settings.warehouse_path),
                    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                    "downcast-ns-timestamp-to-us-on-write": "true",
                }
            )
            logger.info(f"Created Iceberg catalog at {self.settings.warehouse_path}")
        return self._catalog

    def create_namespace(self, namespace: str) -> None:
        """Create namespace (schema) if it doesn't exist.

        Args:
            namespace: Namespace name (e.g., 'bronze', 'silver', 'gold')
        """
        try:
            self.catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Namespace {namespace} already exists")
            else:
                raise

    def get_table(self, namespace: str, table_name: str) -> Optional[Table]:
        """Get an Iceberg table.

        Args:
            namespace: Namespace name
            table_name: Table name

        Returns:
            Iceberg table or None if not exists
        """
        try:
            return self.catalog.load_table(f"{namespace}.{table_name}")
        except NoSuchTableError:
            return None

    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: Schema,
        partition_spec=None,
    ) -> Table:
        """Create a new Iceberg table.

        Args:
            namespace: Namespace name
            table_name: Table name
            schema: Iceberg schema
            partition_spec: Optional partition specification

        Returns:
            Created Iceberg table
        """
        full_table_name = f"{namespace}.{table_name}"
        
        # Drop table if exists (for fresh creation)
        try:
            self.catalog.drop_table(full_table_name)
            logger.info(f"Dropped existing table: {full_table_name}")
        except NoSuchTableError:
            pass

        # Create table with optional partition spec
        if partition_spec:
            table = self.catalog.create_table(
                identifier=full_table_name,
                schema=schema,
                partition_spec=partition_spec,
            )
        else:
            table = self.catalog.create_table(
                identifier=full_table_name,
                schema=schema,
            )
        logger.info(f"Created table: {full_table_name}")
        return table

    def write_dataframe(
        self,
        df: pd.DataFrame,
        namespace: str,
        table_name: str,
        mode: str = "append",
    ) -> int:
        """Write pandas DataFrame to Iceberg table.

        Args:
            df: Pandas DataFrame to write
            namespace: Namespace name
            table_name: Table name
            mode: Write mode ('append' or 'overwrite')

        Returns:
            Number of rows written
        """
        if len(df) == 0:
            logger.warning(f"No data to write to {namespace}.{table_name}")
            return 0

        table = self.get_table(namespace, table_name)
        if table is None:
            raise ValueError(f"Table {namespace}.{table_name} does not exist. Create it first.")

                # Convert pandas DataFrame to PyArrow Table (preserve_index=False to avoid index column)
        # Reset index to avoid __index_level_0__ column
        df_reset = df.reset_index(drop=True)
        temp_table = pa.Table.from_pandas(df_reset, preserve_index=False)
        
        # Build a new schema with proper types
        new_fields = []
        for i, field in enumerate(temp_table.schema):
            if pa.types.is_timestamp(field.type) and field.type.unit == 'ns':
                # Convert timestamp[ns] to date32 for date columns
                new_fields.append(pa.field(field.name, pa.date32()))
            else:
                new_fields.append(field)
        
        new_schema = pa.schema(new_fields)
        arrow_table = temp_table.cast(new_schema)

        # Write to Iceberg
        if mode == "overwrite":
            table.overwrite(arrow_table)
        else:
            table.append(arrow_table)

        logger.info(f"Wrote {len(df)} rows to {namespace}.{table_name}")
        return len(df)

    def read_table(
        self,
        namespace: str,
        table_name: str,
        columns: Optional[list[str]] = None,
        filters: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read Iceberg table into pandas DataFrame.

        Args:
            namespace: Namespace name
            table_name: Table name
            columns: Optional list of columns to read
            filters: Optional filter expression

        Returns:
            Pandas DataFrame
        """
        table = self.get_table(namespace, table_name)
        if table is None:
            logger.warning(f"Table {namespace}.{table_name} does not exist")
            return pd.DataFrame()

        scan = table.scan()
        
        if columns:
            scan = scan.select(*columns)
        
        if filters:
            # PyIceberg filter syntax would go here
            pass

        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()
        
        logger.info(f"Read {len(df)} rows from {namespace}.{table_name}")
        return df

    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            namespace: Namespace name
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        return self.get_table(namespace, table_name) is not None

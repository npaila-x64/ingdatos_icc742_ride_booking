"""CLI to delete all data from the database schemas.

This utility connects to PostgreSQL using the project's settings and truncates
all tables in the selected schemas with `RESTART IDENTITY CASCADE`.

Defaults target the medallion schemas: bronze, silver, gold.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable, List, Tuple

from dotenv import load_dotenv

from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_SCHEMAS = ("bronze", "silver", "gold")


def _get_all_tables(adapter: PostgreSQLAdapter, schemas: Iterable[str]) -> List[Tuple[str, str]]:
    """Return list of (schema, table) for the given schemas."""
    placeholders = ", ".join([f":s_{i}" for i, _ in enumerate(schemas)])
    params = {f"s_{i}": s for i, s in enumerate(schemas)}
    query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ({placeholders})
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
    """
    rows = adapter.execute_query(query, params)
    return [(r["table_schema"], r["table_name"]) for r in rows]


def _truncate_tables(adapter: PostgreSQLAdapter, fq_tables: List[Tuple[str, str]]) -> None:
    """Truncate tables with RESTART IDENTITY CASCADE.

    Uses a single TRUNCATE statement for efficiency; splits into batches if needed.
    """
    if not fq_tables:
        logger.info("No tables found to truncate.")
        return

    # Build fully qualified names and chunk to avoid exceeding statement size in large DBs
    fq_names = [f'{schema}.{table}' for schema, table in fq_tables]

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    # Conservative chunk size; Postgres can handle much larger, but this is safe
    for batch in chunks(fq_names, 100):
        stmt = f"TRUNCATE TABLE {', '.join(batch)} RESTART IDENTITY CASCADE"
        logger.info("Truncating %d tables...", len(batch))
        adapter.execute_statement(stmt)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete ALL data from selected schemas by truncating their tables. "
            "Defaults to bronze, silver, gold."
        )
    )

    parser.add_argument(
        "--schemas",
        "-s",
        nargs="+",
        default=list(DEFAULT_SCHEMAS),
        help="Schemas to wipe (space-separated). Defaults: bronze silver gold",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be truncated without executing",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Required to actually perform deletion",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load env and settings
    load_dotenv()
    settings = load_settings()

    target_schemas = tuple(dict.fromkeys(s.strip() for s in args.schemas if s.strip()))
    if not target_schemas:
        logger.error("No schemas specified.")
        sys.exit(2)

    logger.info("Targeting schemas: %s", ", ".join(target_schemas))

    with PostgreSQLAdapter(settings.database) as adapter:
        tables = _get_all_tables(adapter, target_schemas)
        if not tables:
            logger.info("No tables found in selected schemas.")
            return

        logger.info("Found %d tables to truncate.", len(tables))
        for schema, table in tables:
            logger.info(" - %s.%s", schema, table)

        if args.dry_run and not args.force:
            logger.info("Dry-run mode: no changes applied. Use --force to execute.")
            return

        if not args.force:
            logger.error("Refusing to delete without --force. Re-run with --force to proceed.")
            sys.exit(3)

        _truncate_tables(adapter, tables)
        logger.info("Database wipe completed for: %s", ", ".join(target_schemas))


if __name__ == "__main__":
    main()


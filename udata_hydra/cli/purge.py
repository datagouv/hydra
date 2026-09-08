from datetime import datetime, timedelta, timezone
from typing import Any, Coroutine

import typer
from asyncpg import Record

from udata_hydra import config
from udata_hydra.cli.common import _make_async_wrapper, cli, connection, log
from udata_hydra.logger import quiet_logs


async def _purge_checks(
    retention_days: int = 60,
    quiet: bool = False,
) -> None:
    """Delete outdated checks that are more than `retention_days` days old"""
    with quiet_logs(enabled=quiet):
        conn = await connection()
        log.debug(f"Deleting checks that are more than {retention_days} days old...")
        res: Record = await conn.fetchrow(
            f"""WITH deleted AS (DELETE FROM checks WHERE created_at < now() - interval '{retention_days} days' RETURNING *) SELECT count(*) FROM deleted"""
        )
        deleted: int = res["count"]
        log.info(f"Deleted {deleted} checks.")


@cli.command()
def purge_checks(
    retention_days: int = typer.Option(60, help="Number of days to keep checks"),
    quiet: bool = typer.Option(False, help="Ignore logs except for errors"),
) -> Coroutine[Any, Any, None]:
    """Delete outdated checks that are more than `retention_days` days old"""
    return _make_async_wrapper(_purge_checks)(retention_days=retention_days, quiet=quiet)


async def _purge_csv_tables(
    quiet: bool = False,
    hard_delete: bool = False,
) -> None:
    """Delete converted CSV tables for resources url no longer in catalog"""
    # TODO: check if we should use parsing_table from table_index?
    # And are they necessarily in sync?

    # We want to delete all tables which names (from md5(url)) don't match any URL in catalog
    with quiet_logs(enabled=quiet):
        q_catalog = "SELECT DISTINCT md5(url) AS parsing_table FROM catalog WHERE deleted IS false;"
        conn_main = await connection()
        res_catalog: list[Record] = await conn_main.fetch(q_catalog)
        catalog_tables: set[str] = set([r["parsing_table"] for r in res_catalog])

        # only including the parsing tables (hopefully the conditions are restrictive enough)
        q_tables = f"""SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname = '{config.DATABASE_SCHEMA}'
        AND LENGTH(tablename) = 32
        AND tablename ~ '[0-9]';"""
        conn_csv = await connection(db_name="csv")
        res_tables: list[Record] = await conn_csv.fetch(q_tables)
        parsing_tables: set[str] = set([r["tablename"] for r in res_tables])

        tables_to_delete: set[str] = parsing_tables - catalog_tables

        success_count = 0
        error_count = 0
        log.debug(f"{len(tables_to_delete)} tables to delete")
        for table in tables_to_delete:
            try:
                async with conn_main.transaction():
                    async with conn_csv.transaction():
                        log.debug(f'Deleting table "{table}"')
                        await conn_csv.execute(f'DROP TABLE IF EXISTS "{table}"')
                        if hard_delete:
                            await conn_csv.execute(
                                "DELETE FROM tables_index WHERE parsing_table = $1", table
                            )
                        else:
                            await conn_csv.execute(
                                "UPDATE tables_index SET deleted_at = NOW() WHERE parsing_table = $1",
                                table,
                            )
                        await conn_main.execute(
                            "UPDATE checks SET parsing_table = NULL WHERE parsing_table = $1", table
                        )
                        success_count += 1
            except Exception as e:
                error_count += 1
                log.error(f'Failed to delete table "{table}": {str(e)}')
                continue

        if success_count:
            log.info(f"Successfully deleted {success_count} table(s).")
        if error_count:
            log.warning(f"Failed to delete {error_count} table(s). Check logs for details.")
        if not (success_count or error_count):
            log.info("Nothing to delete.")


@cli.command()
def purge_csv_tables(
    quiet: bool = typer.Option(False, help="Ignore logs except for errors"),
    hard_delete: bool = False,
) -> Coroutine[Any, Any, None]:
    """Delete converted CSV tables for resources url no longer in catalog"""
    return _make_async_wrapper(_purge_csv_tables)(quiet=quiet, hard_delete=hard_delete)


async def _purge_selected_csv_tables(
    retention_days: int | None = None,
    retention_tables: int | None = None,
    quiet: bool = False,
) -> None:
    """Delete converted CSV tables either:
    - if they're more than retention_days days old
    - if they're not in the top retention_tables most recent
    """
    with quiet_logs(enabled=quiet):
        assert retention_days is not None or retention_tables is not None
        conn_csv = await connection(db_name="csv")
        if retention_days is not None:
            threshold = datetime.now(timezone.utc) - timedelta(days=int(retention_days))
            q = """SELECT DISTINCT parsing_table FROM tables_index WHERE created_at <= $1"""
            res: list[Record] = await conn_csv.fetch(q, threshold)
        elif retention_tables is not None:
            q = """SELECT DISTINCT ON (created_at) parsing_table FROM tables_index ORDER BY created_at DESC OFFSET $1"""
            res: list[Record] = await conn_csv.fetch(q, int(retention_tables))

        tables_to_delete: list[str] = [r["parsing_table"] for r in res]

        success_count = 0
        error_count = 0
        conn_main = await connection()
        for table in tables_to_delete:
            try:
                async with conn_main.transaction():
                    async with conn_csv.transaction():
                        log.debug(f'Deleting table "{table}"')
                        await conn_csv.execute(f'DROP TABLE IF EXISTS "{table}"')
                        await conn_csv.execute(
                            "DELETE FROM tables_index WHERE parsing_table = $1", table
                        )
                        await conn_main.execute(
                            "UPDATE checks SET parsing_table = NULL WHERE parsing_table = $1", table
                        )
                        success_count += 1
            except Exception as e:
                error_count += 1
                log.error(f'Failed to delete table "{table}": {str(e)}')
                continue

        if success_count:
            log.info(f"Successfully deleted {success_count} table(s).")
        if error_count:
            log.warning(f"Failed to delete {error_count} table(s). Check logs for details.")
        if not (success_count or error_count):
            log.info("Nothing to delete.")


@cli.command()
def purge_selected_csv_tables(
    retention_days: int | None = None,
    retention_tables: int | None = None,
    quiet: bool = typer.Option(False, help="Ignore logs except for errors"),
) -> Coroutine[Any, Any, None]:
    """Delete converted CSV tables either:
    - if they're more than retention_days days old
    - if they're not in the top retention_tables most recent
    """
    return _make_async_wrapper(_purge_selected_csv_tables)(
        retention_days=retention_days, retention_tables=retention_tables, quiet=quiet
    )


async def _purge_shadow_tables(
    quiet: bool = False,
) -> None:
    """Drop all leftover shadow tables"""
    with quiet_logs(enabled=quiet):
        conn = await connection(db_name="csv")
        res: list[Record] = await conn.fetch(
            f"""SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = '{config.DATABASE_SCHEMA}'
            AND tablename ~ '^[0-9a-f]{{32}}_s[0-9a-f]{{4}}$'"""
        )

        if not res:
            log.info("No shadow tables to purge.")
            return

        success_count = 0
        error_count = 0
        for row in res:
            try:
                await conn.execute(f'DROP TABLE IF EXISTS "{row["tablename"]}"')
                success_count += 1
            except Exception as e:
                error_count += 1
                log.error(f'Failed to drop shadow table "{row["tablename"]}": {e}')
                continue

        if success_count:
            log.info(f"Dropped {success_count} shadow table(s).")
        if error_count:
            log.warning(f"Failed to drop {error_count} shadow table(s).")


@cli.command()
def purge_shadow_tables(
    quiet: bool = typer.Option(False, help="Ignore logs except for errors"),
) -> Coroutine[Any, Any, None]:
    """Drop leftover shadow tables. You should stop workers first to prevent deleting an in-use shadow table."""
    typer.echo(
        "WARNING: Make sure no workers are currently importing data, "
        "otherwise an in-use shadow table could be dropped."
    )
    typer.confirm("Continue?", abort=True)
    return _make_async_wrapper(_purge_shadow_tables)(quiet=quiet)

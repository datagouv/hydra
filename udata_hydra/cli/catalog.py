import csv
import logging
import os
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg
import typer
from progressist import ProgressBar

from udata_hydra import config
from udata_hydra.cli.common import _make_async_wrapper, cli, connection, log
from udata_hydra.cli.db import _drop_dbs, _migrate
from udata_hydra.db.resource import Resource
from udata_hydra.logger import quiet_logs
from udata_hydra.utils import download_file


async def _load_catalog(
    url: str | None = None,
    drop_meta: bool = False,
    drop_all: bool = False,
    quiet: bool = False,
):
    """Load the catalog into DB from CSV file"""
    with quiet_logs(enabled=quiet):
        if not url:
            url = config.CATALOG_URL

        if drop_meta or drop_all:
            dbs = ["main"] if drop_meta else ["main", "csv"]
            await _drop_dbs(dbs=dbs)
            await _migrate()

        def iter_with_progressbar_or_quiet(rows, quiet):
            if quiet:
                for row in rows:
                    yield row
            else:
                bar = ProgressBar(total=len(rows))
                for row in bar.iter(rows):
                    yield row

        try:
            log.info(f"Downloading resources catalog from {url}...")
            with NamedTemporaryFile(
                dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, delete=False
            ) as fd:
                await download_file(url, fd)
            log.info("Upserting resources catalog in database...")
            # consider everything deleted, deleted will be updated when loading new catalog
            conn = await connection()
            await conn.execute("UPDATE catalog SET deleted = TRUE")
            with open(fd.name) as fd:
                reader = csv.DictReader(fd, delimiter=";")
                rows = list(reader)
                for row in iter_with_progressbar_or_quiet(rows, quiet):
                    if row.get("dataset.archived") != "False":
                        continue

                    await conn.execute(
                        """
                        INSERT INTO catalog (
                            dataset_id, resource_id, url, type, format,
                            harvest_modified_at, title, deleted, priority, status
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, FALSE, '{}'::jsonb)
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = $1,
                            url = $3,
                            deleted = FALSE,
                            type = $4,
                            format = $5,
                            harvest_modified_at = $6,
                            title = $7;
                    """,
                        row["dataset.id"],
                        row["id"],
                        row["url"],
                        row["type"],
                        row["format"],
                        # force timezone info to UTC (catalog data should be in UTC)
                        datetime.fromisoformat(row["harvest.modified_at"]).replace(
                            tzinfo=timezone.utc
                        )
                        if row["harvest.modified_at"]
                        else None,
                        row["title"],
                    )
            log.info("Resources catalog successfully upserted into DB.")
            cleaned_count: int = await Resource.clean_up_statuses()
            log.info(f" {cleaned_count} stuck job statuses successfully cleared.")
        except Exception as e:
            raise e
        finally:
            fd.close()
            os.unlink(fd.name)


@cli.command()
def load_catalog(
    url: str | None = typer.Option(
        None, help="URL of the catalog to fetch, by default defined in config"
    ),
    drop_meta: bool = typer.Option(False, help="Drop the metadata tables (catalog, checks...)"),
    drop_all: bool = typer.Option(False, help="Drop metadata tables and parsed csv content"),
    quiet: bool = typer.Option(False, help="Ignore logs except for errors"),
):
    """Load the catalog into DB from CSV file"""
    return _make_async_wrapper(_load_catalog)(
        url=url, drop_meta=drop_meta, drop_all=drop_all, quiet=quiet
    )


async def _insert_resource_into_catalog(resource_id: str):
    """Insert a resource into the catalog
    Useful for local tests, instead of having to resync the whole catalog for one new resource

    :resource_id: id of the resource to insert
    """
    existing_resource: asyncpg.Record | None = await Resource.get(resource_id)
    action = "insert"
    if existing_resource:
        logging.warning("Resource already exists in catalog, updating...")
        action = "updat"
    url = f"https://www.data.gouv.fr/api/2/datasets/resources/{resource_id}/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            resource = await resp.json()
    try:
        conn = await connection()
        await conn.execute(
            """
            INSERT INTO catalog (
                dataset_id, resource_id, url, title, harvest_modified_at,
                deleted, priority, status
            )
            VALUES ($1, $2, $3, $4, $5, FALSE, FALSE, '{}'::jsonb)
            ON CONFLICT (resource_id) DO UPDATE SET
                dataset_id = $1,
                url = $3,
                title = $4,
                deleted = FALSE;
            """,
            resource["dataset_id"],
            resource["resource"]["id"],
            resource["resource"]["url"],
            resource["resource"].get("title"),
            # force timezone info to UTC (catalog data should be in UTC)
            datetime.fromisoformat(resource["resource"]["harvest"]["modified_at"]).replace(
                tzinfo=timezone.utc
            )
            if (
                resource["resource"].get("harvest") is not None
                and resource["resource"]["harvest"].get("modified_at")
            )
            else None,
        )
        log.info(f"Resource {resource_id} successfully {action}ed into DB.")
    except Exception as e:
        raise e


@cli.command()
def insert_resource_into_catalog(resource_id: str):
    """Insert a resource into the catalog
    Useful for local tests, instead of having to resync the whole catalog for one new resource

    :resource_id: id of the resource to insert
    """
    return _make_async_wrapper(_insert_resource_into_catalog)(resource_id=resource_id)


async def _insert_url_into_catalog(url: str, resource_id: str):
    """Insert a URL into the catalog
    Useful for local tests, instead of having to resync the whole catalog for one new URL

    :url: URL of the resource to insert
    :resource_id: resource ID (mandatory)
    """
    # Check if resource already exists
    existing_resource: asyncpg.Record | None = await Resource.get(resource_id)
    action = "insert"
    if existing_resource:
        logging.warning("Resource already exists in catalog, updating...")
        action = "updat"
    try:
        conn = await connection()
        await conn.execute(
            """
            INSERT INTO catalog (
                dataset_id, resource_id, url, type, format,
                harvest_modified_at, deleted, priority, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, '{}'::jsonb)
            ON CONFLICT (resource_id) DO UPDATE SET
                dataset_id = $1,
                url = $3,
                deleted = FALSE,
                type = $4,
                format = $5,
                harvest_modified_at = $6;
            """,
            "temp_external",  # fixed dataset_id for external analysis
            resource_id,
            url,
            "main",  # default type
            "csv",  # default format, can be overridden later
            datetime.now(timezone.utc),  # current timestamp as harvest_modified_at
        )
        log.info(f"URL {url} successfully {action}ed into DB with resource_id {resource_id}.")
    except Exception as e:
        raise e


@cli.command()
def insert_url_into_catalog(url: str, resource_id: str):
    """Insert a URL into the catalog
    Useful for local tests, instead of having to resync the whole catalog for one new URL

    :url: URL of the resource to insert
    :resource_id: resource ID (mandatory)
    """
    return _make_async_wrapper(_insert_url_into_catalog)(url=url, resource_id=resource_id)

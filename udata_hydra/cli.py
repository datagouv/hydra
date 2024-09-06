import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import aiohttp
from asyncpg import Record, connect
from humanfriendly import parse_size
from minicli import cli, run, wrap
from progressist import ProgressBar
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateIndex, CreateTable, Index, PrimaryKeyConstraint

from udata_hydra import config
from udata_hydra.analysis.csv import PYTHON_TYPE_TO_PG, PYTHON_TYPE_TO_PY, analyse_csv, delete_table
from udata_hydra.crawl.check_resources import check_resource as crawl_check_resource
from udata_hydra.db.resource import Resource
from udata_hydra.logger import setup_logging
from udata_hydra.migrations import Migrator
from udata_hydra.utils.db import get_columns_with_indexes

context = {}
log = setup_logging()


async def download_file(url: str, fd):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            while True:
                chunk = await resp.content.read(1024)
                if not chunk:
                    break
                fd.write(chunk)


async def connection(db_name: str = "main"):
    if db_name not in context["conn"]:
        dsn = (
            config.DATABASE_URL
            if db_name == "main"
            else getattr(config, f"DATABASE_URL_{db_name.upper()}")
        )
        context["conn"][db_name] = await connect(
            dsn=dsn, server_settings={"search_path": config.DATABASE_SCHEMA}
        )
    return context["conn"][db_name]


@cli
async def load_catalog(
    url: str | None = None, drop_meta: bool = False, drop_all: bool = False, quiet: bool = False
):
    """Load the catalog into DB from CSV file

    :url: URL of the catalog to fetch, by default defined in config
    :drop_meta: drop the metadata tables (catalog, checks...)
    :drop_all: drop metadata tables and parsed csv content
    :quiet: ingore logs except for errors
    """
    if quiet:
        log.setLevel(logging.ERROR)

    if not url:
        url = config.CATALOG_URL

    if drop_meta or drop_all:
        dbs = ["main"] if drop_meta else ["main", "csv"]
        await drop_dbs(dbs=dbs)
        await migrate()

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
        with NamedTemporaryFile(dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, delete=False) as fd:
            await download_file(url, fd)
        log.info("Upserting resources catalog in database...")
        # consider everything deleted, deleted will be updated when loading new catalog
        conn = await connection()
        await conn.execute("UPDATE catalog SET deleted = TRUE")
        with open(fd.name) as fd:
            reader = csv.DictReader(fd, delimiter=";")
            rows = list(reader)
            for row in iter_with_progressbar_or_quiet(rows, quiet):
                # Skip resources belonging to an archived dataset
                if row.get("dataset.archived") != "False":
                    continue

                await conn.execute(
                    """
                    INSERT INTO catalog (
                        dataset_id, resource_id, url, harvest_modified_at,
                        deleted, priority, status
                    )
                    VALUES ($1, $2, $3, $4, FALSE, FALSE, NULL)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        deleted = FALSE;
                """,
                    row["dataset.id"],
                    row["id"],
                    row["url"],
                    # force timezone info to UTC (catalog data should be in UTC)
                    datetime.fromisoformat(row["harvest.modified_at"]).replace(tzinfo=timezone.utc)
                    if row["harvest.modified_at"]
                    else None,
                )
        log.info("Resources catalog successfully upserted into DB.")
    except Exception as e:
        raise e
    finally:
        fd.close()
        os.unlink(fd.name)


@cli
async def crawl_url(url: str, method: str = "get"):
    """Quickly crawl an URL"""
    log.info(f"Checking url {url}")
    async with aiohttp.ClientSession(timeout=None) as session:
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        try:
            async with _method(url, timeout=timeout, allow_redirects=True) as resp:
                print("Status :", resp.status)
                print("Headers:", resp.headers)
        except Exception as e:
            log.error(e)


@cli
async def check_resource(resource_id: str, method: str = "get"):
    """Trigger a complete check for a given resource_id"""
    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        log.error("Resource not found in catalog")
        return
    async with aiohttp.ClientSession(timeout=None) as session:
        await crawl_check_resource(
            url=resource["url"],
            resource_id=resource_id,
            session=session,
            method=method,
            worker_priority="high",
        )


@cli(name="analyse-csv")
async def analyse_csv_cli(
    check_id: int | None = None, url: str | None = None, debug_insert: bool = False
):
    """Trigger a csv analysis from a check_id or an url"""
    await analyse_csv(check_id=check_id, url=url, debug_insert=debug_insert)


@cli
async def csv_sample(size: int = 1000, download: bool = False, max_size: str = "100M"):
    """Get a csv sample from latest checks

    :size: Size of the sample (how many files to query)
    :download: Download files or just list them
    :max_size: Maximum size for one file (from headers)
    """
    max_size: int = parse_size(max_size)
    start_q = f"""
        SELECT catalog.resource_id, catalog.dataset_id, checks.url,
            checks.headers->>'content-type' as content_type,
            checks.headers->>'content-length' as content_length
        FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND checks.headers->>'content-type' LIKE '%csv%'
        AND checks.status >= 200 and checks.status < 400
        AND CAST(checks.headers->>'content-length' AS INTEGER) <= {max_size}
    """
    end_q = f"""
        ORDER BY RANDOM()
        LIMIT {size / 2}
    """
    # get remote stuff for half the sample
    q = f"""{start_q}
        -- ignore ODS, they're correctly formated from a datastore
        AND checks.url NOT LIKE '%/explore/dataset/%'
        AND checks.url NOT LIKE '%/api/datasets/1.0/%'
        -- ignore ours
        AND checks.domain <> 'static.data.gouv.fr'
        {end_q}
    """
    conn = await connection()
    res = await conn.fetch(q)
    # and from us for the rest
    q = f"""{start_q}
        AND checks.domain = 'static.data.gouv.fr'
        {end_q}
    """
    res += await conn.fetch(q)

    data_path = Path("./data")
    dl_path = data_path / "downloaded"
    dl_path.mkdir(exist_ok=True, parents=True)
    if download:
        log.debug("Cleaning up...")
        (data_path / "_index.csv").unlink(missing_ok=True)
        [p.unlink() for p in Path(dl_path).glob("*.csv")]

    lines = []
    bar = ProgressBar(total=len(res))
    for r in bar.iter(res):
        line = dict(r)
        line["resource_id"] = str(line["resource_id"])
        filename = dl_path / f"{r['dataset_id']}_{r['resource_id']}.csv"
        line["filename"] = filename.__str__()
        lines.append(line)
        if not download:
            continue
        await download_file(r["url"], filename.open("wb"))
        with os.popen(f"file {filename} -b --mime-type") as proc:
            line["magic_mime"] = proc.read().lower().strip()
        line["real_size"] = filename.stat().st_size

    with (data_path / "_index.csv").open("w") as ofile:
        writer = csv.DictWriter(ofile, fieldnames=lines[0].keys())
        writer.writeheader()
        writer.writerows(lines)


@cli
async def drop_dbs(dbs: list = []):
    for db in dbs:
        conn = await connection(db)
        tables = await conn.fetch(f"""
            SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = '{config.DATABASE_SCHEMA}';
        """)
        for table in tables:
            await conn.execute(f'DROP TABLE "{table["tablename"]}" CASCADE')


@cli
async def migrate(skip_errors: bool = False, dbs: list[str] = ["main", "csv"]):
    """Migrate the database(s)"""
    for db in dbs:
        log.info(f"Migrating db {db}...")
        migrator = await Migrator.create(db, skip_errors=skip_errors)
        await migrator.migrate()


@cli
async def purge_checks(limit: int = 2):
    """Delete past checks for each resource_id, keeping only `limit` number of checks"""
    q = "SELECT resource_id FROM checks GROUP BY resource_id HAVING count(id) > $1"
    conn = await connection()
    resources = await conn.fetch(q, limit)
    for r in resources:
        resource_id = r["resource_id"]
        log.debug(f"Deleting outdated checks for resource {resource_id}")
        q = """DELETE FROM checks WHERE id IN (
            SELECT id FROM checks WHERE resource_id = $1 ORDER BY created_at DESC OFFSET $2
        )
        """
        await conn.execute(q, resource_id, limit)


@cli
async def purge_csv_tables():
    """Delete converted CSV tables for resources url no longer in catalog"""
    # TODO: check if we should use parsing_table from table_index?
    # And are they necessarily in sync?

    # Fetch all parsing tables from checks where we don't have any entry on
    # md5(url) in catalog or all entries are marked as deleted.
    q = """
    SELECT DISTINCT checks.parsing_table
    FROM checks
    LEFT JOIN (
        select url, MAX(id) as id, BOOL_AND(deleted) as deleted
        FROM catalog
        GROUP BY url) c
    ON checks.parsing_table = md5(c.url)
    WHERE checks.parsing_table IS NOT NULL AND (c.id IS NULL OR c.deleted = TRUE);
    """
    conn = await connection()
    tables_to_delete = await conn.fetch(q)
    tables_to_delete = [table["parsing_table"] for table in tables_to_delete]

    for table in tables_to_delete:
        log.debug(f"Deleting table {table}")
        await delete_table(table)
        await conn.execute("UPDATE checks SET parsing_table = NULL WHERE parsing_table = $1", table)
    if len(tables_to_delete):
        log.info(f"Deleted {len(tables_to_delete)} table(s).")
    else:
        log.info("Nothing to delete.")


@cli
async def create_table(table_name: str = "TEST"):
    try:
        conn = await connection()
        await conn.execute(f'DROP TABLE "{table_name}" CASCADE')
    except Exception as e:
        log.error(e)

    metadata = MetaData()
    table: Table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))

    columns = {
        "prenom": "varchar",
        "nom": "varchar",
        "age": "int",
        "siren": "varchar",
    }

    indexes = {
        "siren": "index",
        "nom": "index",
    }

    for col_name, col_type in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_type, String)))

    query = CreateTable(table).compile(dialect=asyncpg.dialect())
    query = query.string.replace("%%", "%")

    if indexes:
        for col_name, index_type in indexes.items():
            if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                log.error(
                    f'Index type "{index_type}" is unknown or not supported yet! Index for colum {col_name} was not created.'
                )
                continue

            else:
                log.debug(f'Add index "{index_type}" on column "{col_name}"')
                if index_type == "index":
                    # Create an index on the column
                    table.append_constraint(Index(f"{table_name}_{col_name}_idx", col_name))

    # Add the index creation queries to the main query
    for index in table.indexes:
        log.debug(f'Creating index "{index_type}" on column "{col_name}"')
        query_index_creation = CreateIndex(index).compile(dialect=asyncpg.dialect())
        query_index_creation = query_index_creation.string.replace("%%", "%")
        query = query + ";" + query_index_creation

    log.debug(query)
    conn = await connection()
    await conn.execute(query)

    # Check if a index has been created for the table
    indexes: list[Record] | None = await get_columns_with_indexes(table_name)
    assert indexes
    for idx in indexes:
        print(idx)


@wrap
async def cli_wrapper():
    context["conn"] = {}
    yield
    for db in context["conn"]:
        await context["conn"][db].close()


if __name__ == "__main__":
    run()

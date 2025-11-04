import csv
import hashlib
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg
from asyncpg import Record
from humanfriendly import parse_size
from minicli import cli, wrap
from minicli import run as minicli_run
from progressist import ProgressBar

from udata_hydra import config
from udata_hydra.analysis.csv import analyse_csv, csv_detective_routine
from udata_hydra.analysis.geojson import analyse_geojson, csv_to_geojson, geojson_to_pmtiles
from udata_hydra.analysis.parquet import analyse_parquet
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.crawl.check_resources import check_resource as crawl_check_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.logger import setup_logging
from udata_hydra.migrations import Migrator
from udata_hydra.utils import download_file, download_resource

context = {}
log = setup_logging()


async def connection(db_name: str = "main"):
    if db_name not in context["conn"]:
        dsn = (
            config.DATABASE_URL
            if db_name == "main"
            else getattr(config, f"DATABASE_URL_{db_name.upper()}")
        )
        context["conn"][db_name] = await asyncpg.connect(
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
                if row.get("dataset.archived") != "False":
                    continue

                await conn.execute(
                    """
                    INSERT INTO catalog (
                        dataset_id, resource_id, url, type, format,
                        harvest_modified_at, deleted, priority, status
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, NULL)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        deleted = FALSE,
                        type = $4,
                        format = $5,
                        harvest_modified_at = $6;
                """,
                    row["dataset.id"],
                    row["id"],
                    row["url"],
                    row["type"],
                    row["format"],
                    # force timezone info to UTC (catalog data should be in UTC)
                    datetime.fromisoformat(row["harvest.modified_at"]).replace(tzinfo=timezone.utc)
                    if row["harvest.modified_at"]
                    else None,
                )
        log.info("Resources catalog successfully upserted into DB.")
        await Resource.clean_up_statuses()
        log.info("Stuck statuses sucessfully reset to null.")
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


@cli(name="download-resource")
async def download_resource_cli(resource_id: str, output_dir: str | None = None):
    """Download a resource from the catalog

    :resource_id: ID of the resource to download
    :output_dir: Custom output directory (defaults to TEMPORARY_DOWNLOAD_FOLDER)
    """
    resource: asyncpg.Record | None = await Resource.get(resource_id)
    if not resource:
        log.error(f"Resource {resource_id} not found in catalog")
        return

    try:
        tmp_file, file_extension = await download_resource(resource["url"])
        output_path = (
            Path(output_dir or config.TEMPORARY_DOWNLOAD_FOLDER or ".")
            / f"{resource_id}{file_extension}"
        )
        # Move the temporary file to the desired output location
        Path(tmp_file.name).rename(output_path)
        log.info(f"Successfully downloaded resource {resource_id} to {output_path}")
    except Exception as e:
        log.error(f"Failed to download resource {resource_id}: {e}")
        raise


@cli
async def check_resource(resource_id: str, method: str = "get", force_analysis: bool = True):
    """Trigger a complete check for a given resource_id"""
    resource: asyncpg.Record | None = await Resource.get(resource_id)
    if not resource:
        log.error(f"Resource {resource_id} not found in catalog")
        return
    async with aiohttp.ClientSession(timeout=None) as session:
        await crawl_check_resource(
            url=resource["url"],
            resource=resource,
            session=session,
            method=method,
            force_analysis=force_analysis,
            worker_priority="high",
        )


@cli(name="analyse-resource")
async def analyse_resource_cli(resource_id: str):
    """Trigger a resource analysis, mainly useful for local debug (with breakpoints)"""
    check: Record | None = await Check.get_by_resource_id(resource_id)
    if not check:
        log.error("Could not find a check linked to the specified resource ID")
        return
    await analyse_resource(check=check, last_check=None, force_analysis=True)


@cli(name="analyse-csv")
async def analyse_csv_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
    debug_insert: bool = False,
):
    """Trigger a csv analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    assert check_id or url or resource_id

    # Try to get check from different sources
    check = None
    tmp_resource_id = None

    # Try to get check from check_id
    if check_id:
        record = await Check.get_by_id(int(check_id), with_deleted=True)
        check = dict(record) if record else None

    # Try to get check from URL
    if not check and url:
        records = await Check.get_by_url(url)
        if records:
            if len(records) > 1:
                log.warning(f"Multiple checks found for URL {url}, using the latest one")
            check = dict(records[0])

    # Try to get check from resource_id
    if not check and resource_id:
        record = await Check.get_by_resource_id(resource_id)
        check = dict(record) if record else None

    # We cannot get a check, it's an external URL analysis, we need to create a temporary check
    if not check and url:
        tmp_resource_id = str(uuid.uuid4())
        await insert_url_into_catalog(url=url, resource_id=tmp_resource_id)
        check = await Check.insert(
            {
                "resource_id": tmp_resource_id,
                "url": url,
                "status": 200,
                "headers": {},
                "timeout": False,
            },
            returning="*",
        )

    elif not check:
        log.error("Could not find a check for the specified parameters")
        return

    await analyse_csv(check=check, debug_insert=debug_insert)
    log.info("CSV analysis completed")

    if url and tmp_resource_id:
        # Clean up temporary data created for analysis with external URL
        try:
            # Clean up CSV database tables
            csv_pool = await connection(db_name="csv")
            table_hash = hashlib.md5(url.encode()).hexdigest()

            await csv_pool.execute(f'DROP TABLE IF EXISTS "{table_hash}"')
            await csv_pool.execute(f"DELETE FROM tables_index WHERE parsing_table='{table_hash}'")

            # Clean up the temporary resource and temporary check from catalog
            check = await Check.get_by_resource_id(tmp_resource_id)
            if check:
                await Check.delete(check["id"])
            await Resource.delete(resource_id=tmp_resource_id, hard_delete=True)

            # Clean up MinIO files if any (parquet, etc.)
            # Note: This would require additional MinIO cleanup logic

            log.info(f"Cleaned up temporary data for {url}")
        except Exception as e:
            log.warning(f"Failed to clean temporary external data for {url}: {e}")


@cli(name="analyse-geojson")
async def analyse_geojson_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a GeoJSON analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    assert check_id or url or resource_id
    check = None
    if check_id:
        check: Record | None = await Check.get_by_id(int(check_id), with_deleted=True)
    if not check and url:
        checks: list[Record] | None = await Check.get_by_url(url)
        if checks and len(checks) > 1:
            log.warning(f"Multiple checks found for URL {url}, using the latest one")
        check = checks[0] if checks else None
    if not check and resource_id:
        check: Record | None = await Check.get_by_resource_id(resource_id)
    if not check:
        if check_id:
            log.error("Could not retrieve the specified check")
        elif url:
            log.error("Could not find a check linked to the specified URL")
        elif resource_id:
            log.error("Could not find a check linked to the specified resource ID")
        return
    await analyse_geojson(check=dict(check))


@cli(name="convert-csv-to-geojson")
async def convert_csv_to_geojson_cli(csv_filepath: str):
    """Convert a CSV file to GeoJSON format using udata-hydra analysis functions.

    :csv_filepath: Path to the CSV file to convert
    """

    csv_path = Path(csv_filepath)
    geojson_filepath = Path(f"{csv_path.stem}.geojson")

    if not csv_path.exists():
        log.error(f"CSV file not found: {csv_path}")
        return

    file_size = csv_path.stat().st_size
    log.info(f"Processing CSV file: {csv_path}")
    log.info(f"File size: {file_size} bytes")

    # Analyze the CSV with csv_detective
    log.info("Analyzing CSV structure...")
    try:
        # csv_detective handles encoding detection automatically
        inspection, df = csv_detective_routine(
            file_path=str(csv_path),
            output_profile=True,
            output_df=True,
            cast_json=False,
            num_rows=-1,
            save_results=False,
            verbose=True,
        )

        log.info(f"CSV analysis complete. Found {len(df)} rows and {len(df.columns)} columns")
        log.info(f"Columns: {list(df.columns)}")

        # Show column formats for debugging
        log.info("Column formats detected:")
        for column, detection in inspection["columns"].items():
            log.info(f"  {column}: {detection['format']}")

        # Convert to GeoJSON
        log.info("Converting to GeoJSON...")

        try:
            # Convert to GeoJSON (no MinIO upload, no database updates)
            result = await csv_to_geojson(
                df=df,
                inspection=inspection,
                output_file_path=geojson_filepath,
                upload_to_minio=False,
            )

            if result:
                geojson_size, geojson_url = result
                log.info("Conversion successful!")
                log.info(f"GeoJSON file: {geojson_filepath}")
                log.info(f"GeoJSON file size: {geojson_size} bytes")
                log.info(f"GeoJSON file URL: {geojson_url}")
            else:
                log.warning("No geographical data found in CSV, skipping conversion")

        except Exception as e:
            log.error(f"Error during GeoJSON conversion: {e}")
            import traceback

            traceback.print_exc()

    except Exception as e:
        log.error(f"Error during CSV analysis: {e}")
        import traceback

        traceback.print_exc()


@cli(name="convert-geojson-to-pmtiles")
async def convert_geojson_to_pmtiles_cli(geojson_filepath: str):
    """Convert a GeoJSON file to PMTiles format using udata-hydra analysis functions.

    :geojson_filepath: Path to the GeoJSON file to convert
    """
    geojson_path = Path(geojson_filepath)

    if not geojson_path.exists():
        log.error(f"GeoJSON file not found: {geojson_path}")
        return

    file_size = geojson_path.stat().st_size
    log.info(f"Processing GeoJSON file: {geojson_path}")
    log.info(f"File size: {file_size} bytes")

    # Convert to PMTiles
    log.info("Converting to PMTiles...")

    pmtiles_filepath = Path(f"{geojson_path.stem}.pmtiles")

    try:
        # Convert to PMTiles (no MinIO upload, no database updates)
        pmtiles_size, pmtiles_url = await geojson_to_pmtiles(
            input_file_path=geojson_path, output_file_path=pmtiles_filepath, upload_to_minio=False
        )

        log.info("Conversion successful!")
        log.info(f"PMTiles file: {pmtiles_filepath}")
        log.info(f"PMTiles file size: {pmtiles_size} bytes")
        log.info(f"PMTiles file URL: {pmtiles_url}")

    except Exception as e:
        log.error(f"Error during PMTiles conversion: {e}")
        import traceback

        traceback.print_exc()


@cli(name="analyse-parquet")
async def analyse_parquet_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a parquet analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    assert check_id or url or resource_id
    check = None
    if check_id:
        check: Record | None = await Check.get_by_id(int(check_id), with_deleted=True)
    if not check and url:
        checks: list[Record] | None = await Check.get_by_url(url)
        if checks and len(checks) > 1:
            log.warning(f"Multiple checks found for URL {url}, using the latest one")
        check = checks[0] if checks else None
    if not check and resource_id:
        check: Record | None = await Check.get_by_resource_id(resource_id)
    if not check:
        if check_id:
            log.error("Could not retrieve the specified check")
        elif url:
            log.error("Could not find a check linked to the specified URL")
        elif resource_id:
            log.error("Could not find a check linked to the specified resource ID")
        return
    await analyse_parquet(check=dict(check))


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
async def purge_checks(retention_days: int = 60, quiet: bool = False) -> None:
    """Delete outdated checks that are more than `retention_days` days old"""
    if quiet:
        log.setLevel(logging.ERROR)

    conn = await connection()
    log.debug(f"Deleting checks that are more than {retention_days} days old...")
    res: Record = await conn.fetchrow(
        f"""WITH deleted AS (DELETE FROM checks WHERE created_at < now() - interval '{retention_days} days' RETURNING *) SELECT count(*) FROM deleted"""
    )
    deleted: int = res["count"]
    log.info(f"Deleted {deleted} checks.")


@cli
async def purge_csv_tables(quiet: bool = False, hard_delete: bool = False) -> None:
    """Delete converted CSV tables for resources url no longer in catalog"""
    # TODO: check if we should use parsing_table from table_index?
    # And are they necessarily in sync?

    # Fetch all parsing tables from checks where we don't have any entry on
    # md5(url) in catalog or all entries are marked as deleted.
    if quiet:
        log.setLevel(logging.ERROR)

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
    conn_main = await connection()
    res: list[Record] = await conn_main.fetch(q)
    tables_to_delete: list[str] = [r["parsing_table"] for r in res]

    success_count = 0
    error_count = 0

    conn_csv = await connection(db_name="csv")
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


@cli
async def insert_resource_into_catalog(resource_id: str):
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
                dataset_id, resource_id, url, harvest_modified_at,
                deleted, priority, status
            )
            VALUES ($1, $2, $3, $4, FALSE, FALSE, NULL)
            ON CONFLICT (resource_id) DO UPDATE SET
                dataset_id = $1,
                url = $3,
                deleted = FALSE;
            """,
            resource["dataset_id"],
            resource["resource"]["id"],
            resource["resource"]["url"],
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


@cli
async def insert_url_into_catalog(url: str, resource_id: str):
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
            VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, NULL)
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


@cli
async def purge_selected_csv_tables(
    retention_days: int | None = None,
    retention_tables: int | None = None,
    quiet: bool = False,
) -> None:
    """Delete converted CSV tables either:
    - if they're more than retention_days days old
    - if they're not in the top retention_tables most recent
    """
    if quiet:
        log.setLevel(logging.ERROR)

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


@wrap
async def cli_wrapper():
    context["conn"] = {}
    yield
    for db in context["conn"]:
        await context["conn"][db].close()


def run():
    """Main CLI entry point"""
    minicli_run()


if __name__ == "__main__":
    run()

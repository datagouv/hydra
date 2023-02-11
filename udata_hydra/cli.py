import csv
import os

from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg

from humanfriendly import parse_size
from minicli import cli, run, wrap
from progressist import ProgressBar

from udata_hydra import config
from udata_hydra.crawl import check_url as crawl_check_url
from udata_hydra.logger import setup_logging
from udata_hydra.migrations import Migrator
from udata_hydra.analysis.csv import analyse_csv, delete_table


context = {}
log = setup_logging()


async def download_file(url, fd):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            while True:
                chunk = await resp.content.read(1024)
                if not chunk:
                    break
                fd.write(chunk)


@cli
async def load_catalog(url=None, drop_meta=False, drop_all=False):
    """Load the catalog into DB from CSV file

    :url: URL of the catalog to fetch, by default defined in config
    :drop_meta: drop the metadata tables (catalog, checks...)
    :drop_all: drop metadata tables and parsed csv content
    """
    if not url:
        url = config.CATALOG_URL

    if drop_meta or drop_all:
        dbs = ["main"] if drop_meta else ["main", "csv"]
        await drop_dbs(dbs=dbs)
        await migrate()

    try:
        log.info(f"Downloading catalog from {url}...")
        with NamedTemporaryFile(delete=False) as fd:
            await download_file(url, fd)
        log.info("Upserting catalog in database...")
        # consider everything deleted, deleted will be updated when loading new catalog
        await context["conn"].execute("UPDATE catalog SET deleted = TRUE")
        with open(fd.name) as fd:
            reader = csv.DictReader(fd, delimiter=";")
            rows = list(reader)
            bar = ProgressBar(total=len(rows))
            for row in bar.iter(rows):
                await context["conn"].execute(
                    """
                    INSERT INTO catalog (
                        dataset_id, resource_id, url, harvest_modified_at,
                        deleted, priority
                    )
                    VALUES ($1, $2, $3, $4, FALSE, FALSE)
                    ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET deleted = FALSE
                """,
                    row["dataset.id"],
                    row["id"],
                    row["url"],
                    # force timezone info to UTC (catalog data should be in UTC)
                    datetime.fromisoformat(row["harvest.modified_at"]).replace(tzinfo=timezone.utc)
                    if row["harvest.modified_at"] else None,
                )
        log.info("Catalog successfully upserted into DB.")
    except Exception as e:
        raise e
    finally:
        fd.close()
        os.unlink(fd.name)


@cli
async def check_url(url, method="get"):
    """Quickly check an URL"""
    log.info(f"Checking url {url}")
    async with aiohttp.ClientSession(timeout=None) as session:
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        try:
            async with _method(
                url, timeout=timeout, allow_redirects=True
            ) as resp:
                print("Status :", resp.status)
                print("Headers:", resp.headers)
        except Exception as e:
            log.error(e)


@cli
async def check_resource(resource_id, method="get"):
    """Trigger a complete check for a given resource_id"""
    q = "SELECT * FROM catalog WHERE resource_id = $1"
    res = await context["conn"].fetch(q, resource_id)
    if not res:
        log.error("Resource not found in catalog")
        return
    async with aiohttp.ClientSession(timeout=None) as session:
        await crawl_check_url(res[0], session, method=method)


@cli(name="analyse-csv")
async def analyse_csv_cli(check_id: int = None, url: str = None, debug_insert=False):
    """Trigger a csv analysis from a check_id or an url"""
    await analyse_csv(check_id=check_id, url=url, debug_insert=debug_insert)


@cli
async def csv_sample(size=1000, download=False, max_size="100M"):
    """Get a csv sample from latest checks

    :size: Size of the sample (how many files to query)
    :download: Download files or just list them
    :max_size: Maximum size for one file (from headers)
    """
    max_size = parse_size(max_size)
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
    res = await context["conn"].fetch(q)
    # and from us for the rest
    q = f"""{start_q}
        AND checks.domain = 'static.data.gouv.fr'
        {end_q}
    """
    res += await context["conn"].fetch(q)

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
async def drop_dbs(dbs=[]):
    for db in dbs:
        if db == "main":
            conn = context["conn"]
        else:
            conn = await asyncpg.connect(dsn=getattr(config, f"DATABASE_URL_{db.upper()}"))

        tables = await conn.fetch("""
            SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname != 'information_schema' AND
            schemaname != 'pg_catalog';
        """)
        for table in tables:
            await conn.execute(f'DROP TABLE "{table["tablename"]}"')


@cli
async def migrate(skip_errors=False, dbs=["main", "csv"]):
    """Migrate the database(s)"""
    for db in dbs:
        log.info(f"Migrating db {db}...")
        migrator = await Migrator.create(db, skip_errors=skip_errors)
        await migrator.migrate()


@cli
async def purge_checks(limit=2):
    """Delete past checks for each resource_id, keeping only `limit` number of checks"""
    q = "SELECT resource_id FROM checks GROUP BY resource_id HAVING count(id) > $1"
    resources = await context["conn"].fetch(q, limit)
    for r in resources:
        resource_id = r["resource_id"]
        log.debug(f"Deleting outdated checks for resource {resource_id}")
        q = """DELETE FROM checks WHERE id IN (
            SELECT id FROM checks WHERE resource_id = $1 ORDER BY created_at DESC OFFSET $2
        )
        """
        await context["conn"].execute(q, resource_id, limit)


@cli
async def purge_csv_tables():
    """Delete converted CSV tables for resources no longer in catalog"""
    q = """
        SELECT parsing_table FROM checks
        WHERE parsing_table IN (
            SELECT md5(url) FROM catalog WHERE deleted = TRUE
        )
    """
    count = 0
    res = await context["conn"].fetch(q)
    for r in res:
        table = r["parsing_table"]
        # check the URL is not used by another active resource
        q = "SELECT id FROM catalog WHERE md5(url) = $1 and deleted = FALSE"
        not_deleted = await context["conn"].fetch(q, table)
        if not not_deleted:
            log.debug(f"Deleting table {table}")
            await delete_table(table)
            await context["conn"].execute(
                "UPDATE checks SET parsing_table = NULL WHERE parsing_table = $1", table
            )
            count += 1
    if count:
        log.info(f"Deleted {count} table(s).")
    else:
        log.info("Nothing to delete.")


@wrap
async def cli_wrapper():
    context["conn"] = await asyncpg.connect(dsn=config.DATABASE_URL)
    yield
    await context["conn"].close()


if __name__ == "__main__":
    run()

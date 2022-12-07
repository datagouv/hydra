import csv
import os

from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg

from asyncpg_trek import plan, execute, Direction
from asyncpg_trek.asyncpg import AsyncpgBackend
from humanfriendly import parse_size
from minicli import cli, run, wrap
from progressist import ProgressBar

from udata_hydra import config
from udata_hydra.crawl import check_url as crawl_check_url
from udata_hydra.logger import setup_logging


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
async def load_catalog(url=None, drop=False):
    """Load the catalog into DB from CSV file"""
    if not url:
        url = config.CATALOG_URL

    if drop:
        await drop_db(["catalog", "checks", "migrations"])
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
                        deleted, priority, initialization
                    )
                    VALUES ($1, $2, $3, $4, FALSE, FALSE, TRUE)
                    ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET deleted = FALSE
                """,
                    row["dataset.id"],
                    row["id"],
                    row["url"],
                    datetime.fromisoformat(row["harvest.modified_at"]) if row["harvest.modified_at"] else None,
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
async def drop_db(tables=["checks", "catalog", "migrations"]):
    for table in tables:
        await context["conn"].execute(f"DROP TABLE IF EXISTS {table}")


@cli
async def migrate(revision=None):
    """Migrate the database to _LATEST_REVISION or specified one"""
    migrations_dir = Path(__file__).parent / "../migrations"

    if not revision:
        with open(migrations_dir / "_LATEST_REVISION", "r") as f:
            revision = f.read().strip()
        log.info(f"No revision asked, using from _LATEST_REVISION: {revision}")

    backend = AsyncpgBackend(context["conn"])
    async with backend.connect() as conn:
        planned = await plan(conn, backend, migrations_dir.resolve(), revision, Direction.up)
        await execute(conn, backend, planned)


@wrap
async def cli_wrapper():
    dsn = config.DATABASE_URL
    context["conn"] = await asyncpg.connect(dsn=dsn)
    context["dsn"] = dsn
    yield
    await context["conn"].close()


if __name__ == "__main__":
    run()

import csv
import os
from pathlib import Path

import typer
from humanfriendly import parse_size
from progressist import ProgressBar

from udata_hydra import config
from udata_hydra.cli.common import _make_async_wrapper, cli, connection, log
from udata_hydra.migrations import Migrator
from udata_hydra.utils import download_file


async def _csv_sample(
    size: int = 1000,
    download: bool = False,
    max_size: str = "100M",
):
    """Get a csv sample from latest checks

    :size: Size of the sample (how many files to query)
    :download: Download files or just list them
    :max_size: Maximum size for one file (from headers)
    """
    max_size_bytes: int = parse_size(max_size)
    start_q = f"""
        SELECT catalog.resource_id, catalog.dataset_id, checks.url,
            checks.headers->>'content-type' as content_type,
            checks.headers->>'content-length' as content_length
        FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND checks.headers->>'content-type' LIKE '%csv%'
        AND checks.status >= 200 and checks.status < 400
        AND CAST(checks.headers->>'content-length' AS INTEGER) <= {max_size_bytes}
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


@cli.command()
def csv_sample(
    size: int = typer.Option(1000, help="Size of the sample (how many files to query)"),
    download: bool = typer.Option(False, help="Download files or just list them"),
    max_size: str = typer.Option("100M", help="Maximum size for one file (from headers)"),
):
    """Get a csv sample from latest checks

    :size: Size of the sample (how many files to query)
    :download: Download files or just list them
    :max_size: Maximum size for one file (from headers)
    """
    return _make_async_wrapper(_csv_sample)(size=size, download=download, max_size=max_size)


async def _drop_dbs(
    dbs: list[str] | None = None,
):
    """Drop tables from specified databases"""
    if dbs is None:
        dbs = ["main"]
    for db in dbs:
        conn = await connection(db)
        tables = await conn.fetch(f"""
            SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = '{config.DATABASE_SCHEMA}';
        """)
        for table in tables:
            await conn.execute(f'DROP TABLE "{table["tablename"]}" CASCADE')


@cli.command()
def drop_dbs(
    dbs: list[str] = typer.Option(["main"], help="List of databases to drop"),
):
    """Drop tables from specified databases"""
    return _make_async_wrapper(_drop_dbs)(dbs=dbs)


async def _migrate(
    skip_errors: bool = False,
    dbs: list[str] | None = None,
):
    """Migrate the database(s)"""
    if dbs is None:
        dbs = ["main", "csv"]
    for db in dbs:
        log.info(f"Migrating db {db}...")
        migrator = await Migrator.create(db, skip_errors=skip_errors)
        await migrator.migrate()


@cli.command()
def migrate(
    skip_errors: bool = typer.Option(False, help="Skip migration errors"),
    dbs: list[str] = typer.Option(["main", "csv"], help="List of databases to migrate"),
):
    """Migrate the database(s)"""
    return _make_async_wrapper(_migrate)(skip_errors=skip_errors, dbs=dbs)

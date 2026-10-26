import csv
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import typer
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.exports import export_parquet
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.analysis.tables_index import get_previous_inspection
from udata_hydra.cli.catalog import _insert_url_into_catalog
from udata_hydra.cli.common import _find_check, _make_async_wrapper, cli, connection, log
from udata_hydra.data_formats import (
    CsvLike,
    Geojson,
    Gz,
    Ogc,
    Parquet,
    Table,
)
from udata_hydra.data_formats.csv_like.to_geojson import csv_to_geojson
from udata_hydra.data_formats.detect import detect_data_format_from_check_or_catalog
from udata_hydra.data_formats.geojson.to_pmtiles import geojson_to_pmtiles
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import download_file, queue, storage_path


async def _analyse_resource_cli(resource_id: str):
    """Trigger a resource analysis, mainly useful for local debug (with breakpoints)"""
    check: Record | None = await Check.get_by_resource_id(resource_id)
    if not check:
        log.error("Could not find a check linked to the specified resource ID")
        return
    await analyse_resource(check=dict(check), force_analysis=True)


@cli.command(name="analyse-resource")
def analyse_resource_cli(resource_id: str):
    """Trigger a resource analysis, mainly useful for local debug (with breakpoints)"""
    return _make_async_wrapper(_analyse_resource_cli)(resource_id=resource_id)


async def _analyse_csv_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
    debug_insert: bool = False,
):
    """Trigger a csv analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    check = await _find_check(check_id=check_id, url=url, resource_id=resource_id)

    # We cannot get a check, it's an external URL analysis, we need to create a temporary check
    tmp_resource_id = None
    if not check and url:
        tmp_resource_id = str(uuid.uuid4())
        await _insert_url_into_catalog(url=url, resource_id=tmp_resource_id)
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

    if not check:
        return

    try:
        data_format = await detect_data_format_from_check_or_catalog(check)
        if data_format is None or not (data_format is Gz or issubclass(data_format, CsvLike)):
            log.error("File does not look like csv-like, aborting")
            return
        try:
            file = await helpers.download_from_check(check, data_format)
        except Exception as e:
            log.warning(f"Failed download resource from  {url}: {e}")
            return
        await file.analyse(check=check, debug_insert=debug_insert)
        log.info("CSV analysis completed")
    finally:
        if url and tmp_resource_id:
            # Clean up temporary data created for analysis with external URL
            try:
                # Clean up CSV database tables
                csv_pool = await connection(db_name="csv")
                table_hash = hashlib.md5(url.encode()).hexdigest()

                await csv_pool.execute(f'DROP TABLE IF EXISTS "{table_hash}"')
                await csv_pool.execute(
                    f"DELETE FROM tables_index WHERE parsing_table='{table_hash}'"
                )

                # Clean up the temporary resource and temporary check from catalog
                check = await Check.get_by_resource_id(tmp_resource_id)
                if check:
                    await Check.delete(check["id"])
                await Resource.delete(resource_id=tmp_resource_id, hard_delete=True)

                # Clean up S3 files if any (parquet, etc.)
                # Note: This would require additional S3 cleanup logic

                log.info(f"Cleaned up temporary data for {url}")
            except Exception as e:
                log.warning(f"Failed to clean temporary external data for {url}: {e}")


@cli.command(name="analyse-csv")
def analyse_csv_cli(
    check_id: str | None = typer.Option(None, help="Check ID to analyze"),
    url: str | None = typer.Option(None, help="URL to analyze"),
    resource_id: str | None = typer.Option(None, help="Resource ID to analyze"),
    debug_insert: bool = typer.Option(False, help="Enable debug mode for insertion"),
):
    """Trigger a csv analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    return _make_async_wrapper(_analyse_csv_cli)(
        check_id=check_id, url=url, resource_id=resource_id, debug_insert=debug_insert
    )


async def _analyse_geojson_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a GeoJSON analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    check = await _find_check(check_id=check_id, url=url, resource_id=resource_id)
    if not check:
        return
    data_format = await detect_data_format_from_check_or_catalog(check)
    if data_format not in (Geojson, Gz):
        log.error("File does not look like geojson, aborting")
        return
    file = await helpers.download_from_check(check, data_format)
    await file.analyse(check=check)


@cli.command(name="analyse-geojson")
def analyse_geojson_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a GeoJSON analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    return _make_async_wrapper(_analyse_geojson_cli)(
        check_id=check_id, url=url, resource_id=resource_id
    )


async def _convert_csv_to_geojson_cli(csv_filepath: str):
    """Convert a CSV file to GeoJSON format using udata-hydra analysis functions.

    :csv_filepath: Path to the CSV file to convert
    """
    csv_file = CsvLike(file_name=csv_filepath)
    log.info(f"Processing CSV-like file: {csv_file.file_name}")
    log.info(f"File size: {csv_file.filesize} bytes")

    # Analyze the CSV with csv_detective
    log.info("Analyzing CSV structure...")
    try:
        # csv_detective handles encoding detection automatically
        inspection = await csv_file.inspect()

        log.info(
            f"CSV analysis complete. Found {inspection['total_lines']} rows and {len(inspection['headers'])} columns"
        )
        log.info(f"Columns: {inspection['headers']}")

        # Show column formats for debugging
        log.info("Column formats detected:")
        for column, detection in inspection["columns"].items():
            log.info(f"  {column}: {detection['format']}")

        # Convert to GeoJSON
        log.info("Converting to GeoJSON...")

        try:
            # Convert to GeoJSON (no S3 upload, no database updates)
            geojson_file = await csv_to_geojson(csv_file)

            if geojson_file:
                log.info("Conversion successful!")
                log.info(f"GeoJSON file: {geojson_file.file_name}")
                log.info(f"GeoJSON file size: {geojson_file.filesize} bytes")
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


@cli.command(name="convert-csv-to-geojson")
def convert_csv_to_geojson_cli(csv_filepath: str):
    """Convert a CSV file to GeoJSON format using udata-hydra analysis functions.

    :csv_filepath: Path to the CSV file to convert
    """
    return _make_async_wrapper(_convert_csv_to_geojson_cli)(csv_filepath=csv_filepath)


async def _convert_geojson_to_pmtiles_cli(geojson_filepath: str):
    """Convert a GeoJSON file to PMTiles format using udata-hydra analysis functions.

    :geojson_filepath: Path to the GeoJSON file to convert
    """
    geojson_file = Geojson(file_name=geojson_filepath)
    log.info(f"Processing GeoJSON file: {geojson_file.file_name}")
    log.info(f"File size: {geojson_file.filesize} bytes")

    # Convert to PMTiles
    log.info("Converting to PMTiles...")

    pmtiles_filepath = Path(f"{Path(geojson_file.file_name).stem}.pmtiles")

    try:
        # Convert to PMTiles (no S3 upload, no database updates)
        pmtiles_file = geojson_to_pmtiles(file=geojson_file)

        log.info("Conversion successful!")
        log.info(f"PMTiles file: {pmtiles_filepath}")
        log.info(f"PMTiles file size: {pmtiles_file.filesize} bytes")

    except Exception as e:
        log.error(f"Error during PMTiles conversion: {e}")
        import traceback

        traceback.print_exc()


@cli.command(name="convert-geojson-to-pmtiles")
def convert_geojson_to_pmtiles_cli(geojson_filepath: str):
    """Convert a GeoJSON file to PMTiles format using udata-hydra analysis functions.

    :geojson_filepath: Path to the GeoJSON file to convert
    """
    return _make_async_wrapper(_convert_geojson_to_pmtiles_cli)(geojson_filepath=geojson_filepath)


async def _analyse_parquet_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a parquet analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    check = await _find_check(check_id=check_id, url=url, resource_id=resource_id)
    if not check:
        return
    tmp_file = await helpers.read_or_download_file(
        check=check,
        filename=None,
        data_format=Parquet,
    )
    file = Parquet(
        file_name=tmp_file.name, resource_id=resource_id, dataset_id=check.get("dataset_id")
    )
    await file.analyse(check=check)


@cli.command(name="analyse-parquet")
def analyse_parquet_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger a parquet analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    return _make_async_wrapper(_analyse_parquet_cli)(
        check_id=check_id, url=url, resource_id=resource_id
    )


async def _analyse_ogc_cli(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
):
    """Trigger an OGC analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    check = await _find_check(check_id=check_id, url=url, resource_id=resource_id)

    if not check and url:
        check = {"id": None, "url": url, "resource_id": None}

    if not check:
        log.warning("Could not find a check relatedto this resource, aborting")
        return

    if not config.OGC_ANALYSIS_ENABLED:
        log.warning("Temporarily enabling OGC analysis for CLI")
        config.override(OGC_ANALYSIS_ENABLED=True)

    data_format = await detect_data_format_from_check_or_catalog(check)
    if data_format is None or not issubclass(data_format, Ogc):
        log.warning("Could not infer an OGC format for the check, aborting")
        return

    result = await data_format.analyse(check)
    if result:
        log.info("OGC analysis completed successfully.")
        log.debug(json.dumps(result, indent=2, default=str, ensure_ascii=False))
    else:
        log.warning("OGC analysis returned no results")


@cli.command(name="analyse-ogc")
def analyse_ogc_cli(
    format: str = typer.Option("wfs", help="The OGC service format to analyse"),
    check_id: str | None = typer.Option(None, help="Check ID to analyze"),
    url: str | None = typer.Option(None, help="OGC endpoint URL to analyze"),
    resource_id: str | None = typer.Option(None, help="Resource ID to analyze"),
):
    """Trigger an OGC analysis from a check_id, an url or a resource_id
    Try to get the check from the check ID, then from the URL
    """
    return _make_async_wrapper(_analyse_ogc_cli)(
        format=format, check_id=check_id, url=url, resource_id=resource_id
    )


# udata extras written by hydra (see helpers.notify_udata). They reflect what data.gouv.fr
# actually exposes, unlike `checks`, where a later crawl without re-parsing gives a new
# last check that has no parsing_table nor parquet_url.
_EXTRA_PARSING_TABLE = "analysis:parsing:parsing_table"
_EXTRA_PARQUET_URL = "analysis:parsing:parquet_url"
_EXTRA_PARSING_ERROR = "analysis:parsing:error"
_BACKFILL_STATUS = "TO_BACKFILL_PARQUET"


def _read_backfill_candidates(catalog_path: str) -> list[tuple[str, str]]:
    """Return (resource_id, parsing_table) for catalog rows whose udata extras show a parsed
    table but no parquet export, sorted by resource_id so that --limit is stable across runs."""
    # the extras column can be very large
    csv.field_size_limit(2**31 - 1)
    candidates = []
    with open(catalog_path, newline="") as fd:
        for row in csv.DictReader(fd, delimiter=";"):
            if row.get("dataset.archived") != "False":
                continue
            if (row.get("format") or "").lower() == "parquet":
                continue
            try:
                resource_id = str(uuid.UUID(row["id"]))
                extras = json.loads(row.get("extras") or "{}")
            except (ValueError, KeyError):
                continue
            if not isinstance(extras, dict):
                continue
            parsing_table = extras.get(_EXTRA_PARSING_TABLE)
            if (
                parsing_table
                and not extras.get(_EXTRA_PARQUET_URL)
                and not extras.get(_EXTRA_PARSING_ERROR)
            ):
                candidates.append((resource_id, parsing_table))
    return sorted(candidates)


async def _backfill_parquet(
    catalog_url: str | None = None,
    catalog_file: str | None = None,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict:
    """Enqueue parquet exports for resources that udata shows as parsed but without parquet."""
    if catalog_file:
        candidates = _read_backfill_candidates(catalog_file)
    else:
        url = catalog_url or config.CATALOG_URL
        log.info(f"Downloading resources catalog from {url}...")
        with NamedTemporaryFile(dir=storage_path("")) as fd:
            await download_file(url, fd)
            fd.flush()
            candidates = _read_backfill_candidates(fd.name)
    log.info(f"{len(candidates)} resources without parquet export in udata extras")

    pool = await context.pool()
    csv_pool = await context.pool("csv")
    skipped: dict[str, int] = {}
    enqueued = 0
    # rows to export, according to csv-detective: a rough measure of the batch size
    total_lines = 0

    def skip(reason: str, resource_id: str) -> None:
        log.debug(f"Skipping parquet backfill for resource {resource_id}: {reason}")
        skipped[reason] = skipped.get(reason, 0) + 1

    for resource_id, parsing_table in candidates:
        resource = await pool.fetchrow(
            "SELECT dataset_id, status FROM catalog WHERE resource_id = $1 AND deleted = FALSE",
            resource_id,
        )
        if resource is None:
            skip("not_in_catalog", resource_id)
            continue
        # BACKOFF only means the crawler waits for the domain to cool down, like get_excluded_clause
        if resource["status"] not in (None, "BACKOFF"):
            skip("busy", resource_id)
            continue

        # the latest table for this resource must be the one udata knows about
        latest_table = await csv_pool.fetchval(
            "SELECT parsing_table FROM tables_index WHERE resource_id = $1 "
            "ORDER BY created_at DESC LIMIT 1",
            resource_id,
        )
        if latest_table != parsing_table:
            skip("table_mismatch", resource_id)
            continue

        # The export writes parquet_url on this check, then sends its whole analysis:parsing
        # group to udata: it must be the check that produced the table, not the last check.
        check = await pool.fetchrow(
            "SELECT * FROM checks WHERE resource_id = $1 AND parsing_table = $2 "
            "ORDER BY id DESC LIMIT 1",
            resource_id,
            parsing_table,
        )
        if check is None:
            skip("no_matching_check", resource_id)
            continue
        # exported since the catalog snapshot was generated
        if check["parquet_url"]:
            skip("already_exported_locally", resource_id)
            continue

        try:
            # JSONB reorders csv_detective keys; the live table has the real column order.
            inspection = await get_previous_inspection(resource_id)
        except Exception:
            log.exception(f"Could not get the inspection of resource {resource_id}")
            inspection = None
        if inspection is None:
            skip("inspection_failed", resource_id)
            continue

        if not dry_run:
            # lock the resource so that the crawler leaves it alone until the export is done,
            # the export job resets the status when it finishes
            locked = await pool.fetchval(
                "UPDATE catalog SET status = $1, status_since = $2 "
                "WHERE resource_id = $3 AND (status IS NULL OR status = 'BACKOFF') "
                "AND deleted = FALSE "
                "RETURNING resource_id",
                _BACKFILL_STATUS,
                datetime.now(timezone.utc),
                resource_id,
            )
            if locked is None:
                skip("busy", resource_id)
                continue
            try:
                queue.enqueue(
                    export_parquet,
                    table=Table(
                        table_name=parsing_table,
                        inspection=inspection,
                        resource_id=resource_id,
                        dataset_id=resource["dataset_id"],
                    ),
                    check=dict(check),
                    _priority="low",
                )
            except Exception:
                await Resource.update(resource_id, {"status": None})
                raise

        if type(inspection.get("total_lines")) is int:
            total_lines += inspection["total_lines"]
        enqueued += 1
        if limit is not None and enqueued >= limit:
            break

    summary = {
        "candidates": len(candidates),
        "enqueued": enqueued,
        "skipped": skipped,
        "total_lines": total_lines,
    }
    log.info(f"Parquet backfill dry_run={dry_run} {summary}")
    return summary


@cli.command(name="backfill-parquet")
def backfill_parquet_cli(
    catalog_url: str | None = typer.Option(
        None, help="URL of the resources catalog to read udata extras from, defaults to config"
    ),
    catalog_file: str | None = typer.Option(
        None, help="Local copy of the resources catalog, used instead of downloading it"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Count candidates and their total lines without locking resources nor enqueueing jobs",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help=(
            "Stop after this many exports. With --dry-run, limits how many candidates are counted. "
            "Exported resources are skipped on the next run"
        ),
    ),
):
    """Enqueue parquet exports for parsed resources that udata shows without a parquet file.

    Candidates come from the udata extras in the resources catalog, then are checked against
    the local tables. Run on a single host. Jobs go to the shared low-priority queue.
    Queued resources get the TO_BACKFILL_PARQUET status until their export is done: do not run
    load-catalog before then, its status clean-up would release them early.
    """
    return _make_async_wrapper(_backfill_parquet)(
        catalog_url=catalog_url, catalog_file=catalog_file, dry_run=dry_run, limit=limit
    )

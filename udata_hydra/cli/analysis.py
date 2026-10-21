import hashlib
import json
import uuid
from pathlib import Path

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
from udata_hydra.utils import queue


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


# Latest check of parsed, non-deleted resources that never got a parquet file.
# Native parquet sources and resources currently being processed are excluded.
_BACKFILL_CANDIDATES_SQL = """
    SELECT checks.id, checks.url, checks.resource_id, checks.parsing_table, catalog.dataset_id
    FROM catalog
    JOIN checks ON checks.id = catalog.last_check
    WHERE catalog.deleted = FALSE
      AND catalog.status IS NULL
      AND lower(catalog.format) IS DISTINCT FROM 'parquet'
      AND checks.parsing_table IS NOT NULL
      AND checks.parquet_url IS NULL
      AND checks.parsing_error IS NULL
    ORDER BY checks.id
"""


async def _backfill_parquet(dry_run: bool = False, limit: int | None = None) -> dict:
    """Enqueue parquet exports for parsed resources that have no parquet file yet."""
    rows = await (await context.pool()).fetch(_BACKFILL_CANDIDATES_SQL)
    skipped = 0
    enqueued = 0
    line_counts: list[int] = []

    for row in rows:
        resource_id = str(row["resource_id"])
        parsing_table = row["parsing_table"]
        try:
            # JSONB reorders csv_detective keys; the live table has the real column order.
            inspection = await get_previous_inspection(resource_id)
        except Exception:
            log.exception(f"Skipping parquet backfill for resource {resource_id}")
            skipped += 1
            continue
        if inspection is None:
            log.warning(
                f"Skipping parquet backfill for resource {resource_id}: table is empty or missing"
            )
            skipped += 1
            continue

        total_lines = inspection.get("total_lines")
        if type(total_lines) is int:
            line_counts.append(total_lines)
        if not dry_run:
            queue.enqueue(
                export_parquet,
                table=Table(
                    table_name=parsing_table,
                    inspection=inspection,
                    resource_id=resource_id,
                    dataset_id=row["dataset_id"],
                ),
                check={
                    "id": row["id"],
                    "url": row["url"],
                    "resource_id": resource_id,
                    "parsing_table": parsing_table,
                },
                _priority="low",
            )
        enqueued += 1
        if limit is not None and enqueued >= limit:
            break

    summary = {
        "skipped": skipped,
        "enqueued": enqueued,
        "min_lines": min(line_counts) if line_counts else None,
        "max_lines": max(line_counts) if line_counts else None,
    }
    log.info(f"Parquet backfill dry_run={dry_run} {summary}")
    return summary


@cli.command(name="backfill-parquet")
def backfill_parquet_cli(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Count candidates and total_lines without enqueueing jobs",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of parquet exports to enqueue. With --dry-run, limits how many candidates are counted",
    ),
):
    """Enqueue parquet exports for parsed resources that do not have a parquet file yet.

    Run once, on a single host, after deploy. Jobs go to the shared low-priority queue.
    """
    return _make_async_wrapper(_backfill_parquet)(dry_run=dry_run, limit=limit)

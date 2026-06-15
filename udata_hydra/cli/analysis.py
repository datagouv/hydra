import hashlib
import json
import uuid
from pathlib import Path

import typer
from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis.csv import analyse_csv, csv_detective_routine
from udata_hydra.analysis.geojson import analyse_geojson
from udata_hydra.analysis.ogc import analyse_ogc
from udata_hydra.analysis.parquet import analyse_parquet
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.cli.catalog import _insert_url_into_catalog
from udata_hydra.cli.common import _find_check, _make_async_wrapper, cli, connection, log
from udata_hydra.conversion.csv_to_geojson import csv_to_geojson
from udata_hydra.conversion.geojson_to_pmtiles import geojson_to_pmtiles
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource


async def _analyse_resource_cli(resource_id: str):
    """Trigger a resource analysis, mainly useful for local debug (with breakpoints)"""
    check: Record | None = await Check.get_by_resource_id(resource_id)
    if not check:
        log.error("Could not find a check linked to the specified resource ID")
        return
    await analyse_resource(check=dict(check), last_check=None, force_analysis=True)


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
    await analyse_geojson(check=check)


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
            cast_json=False,
            num_rows=-1,
            save_results=False,
            verbose=True,
        )

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
            result = await csv_to_geojson(
                file_path=str(csv_path),
                inspection=inspection,
                output_file_path=geojson_filepath,
                upload_to_s3=False,
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
        # Convert to PMTiles (no S3 upload, no database updates)
        pmtiles_size, pmtiles_url = await geojson_to_pmtiles(
            input_file_path=geojson_path, output_file_path=pmtiles_filepath, upload_to_s3=False
        )

        log.info("Conversion successful!")
        log.info(f"PMTiles file: {pmtiles_filepath}")
        log.info(f"PMTiles file size: {pmtiles_size} bytes")
        log.info(f"PMTiles file URL: {pmtiles_url}")

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
    await analyse_parquet(check=check)


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
    format: str,
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
        return

    if not config.OGC_ANALYSIS_ENABLED:
        log.warning("Temporarily enabling OGC analysis for CLI")
        config.override(OGC_ANALYSIS_ENABLED=True)

    result = await analyse_ogc(check=check, format=format)
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

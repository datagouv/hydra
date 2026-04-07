import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.csv_geojson import (
    DEFAULT_GEOJSON_FILEPATH,
    csv_to_geojson,
    csv_to_geojson_from_db,
    minio_client_geojson,
)
from udata_hydra.analysis.pmtiles import (
    DEFAULT_PMTILES_FILEPATH,
    geojson_to_pmtiles,
    minio_client_pmtiles,
)
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
    queue,
    remove_remainders,
)

log = logging.getLogger("udata-hydra")

__all__ = [
    "DEFAULT_GEOJSON_FILEPATH",
    "DEFAULT_PMTILES_FILEPATH",
    "analyse_geojson",
    "csv_to_geojson",
    "csv_to_geojson_and_pmtiles",
    "csv_to_geojson_from_db",
    "export_geojson_for_csv_analysis",
    "export_pmtiles_from_local_geojson",
    "geojson_to_pmtiles",
    "minio_client_geojson",
    "minio_client_pmtiles",
    "task_csv_to_geojson",
    "task_geojson_to_pmtiles",
]


async def analyse_geojson(
    check: Record | dict,
    file_path: str | None = None,
) -> None:
    """Launch GeoJSON analysis from a check or an URL (debug), using previously downloaded file at file_path if any"""
    if not config.GEOJSON_TO_PMTILES:
        log.debug("GEOJSON_TO_PMTILES turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_GEOJSON
    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_GEOJSON"})

    # Check if the resource is in the exceptions table
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

    timer = Timer("analyse-geojson", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    tmp_file = None
    try:
        tmp_file = await helpers.read_or_download_file(
            check=check,
            file_path=file_path,
            file_format="geojson",
            exception=exception,
        )
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore[assignment]

        # Convert to PMTiles
        try:
            pmtiles_filepath = Path(f"{resource_id}.pmtiles")
            pmtiles_size, pmtiles_url = await geojson_to_pmtiles(
                input_file_path=Path(tmp_file.name),
                output_file_path=pmtiles_filepath,
            )
            timer.mark("geojson-to-pmtiles")
        except Exception as e:
            remove_remainders(resource_id, ["pmtiles", "pmtiles-journal"])
            raise ParseException(
                message=str(e),
                step="pmtiles_export",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e

        check = await Check.update(  # type: ignore[assignment]
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
                "pmtiles_url": pmtiles_url,
                "pmtiles_size": pmtiles_size,
            },
        )

    except (ParseException, IOException) as e:
        check = await handle_parse_exception(e, None, check)  # type: ignore[assignment]
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


async def export_geojson_for_csv_analysis(
    *,
    resource_id: str | None,
    check_id: int | None,
    inspection: dict,
    geojson_filepath: Path,
    file_path: str | None,
    table_name: str | None,
    timer: Timer | None = None,
) -> tuple[int, str | None] | None:
    """Build GeoJSON from the parsing table (Hydra pipeline) or from a CSV file (CLI/tests).

    When ``table_name`` is set, reads features from PostgreSQL via ``csv_to_geojson_from_db``.
    Otherwise requires ``file_path`` for ``csv_to_geojson``.

    Returns ``(geojson_size, geojson_url)`` or ``None`` when ``DB_TO_GEOJSON`` is false or
    there are no geographical columns.
    """
    if not config.DB_TO_GEOJSON:
        return None

    if resource_id:
        # Update resource status for GeoJSON conversion
        await Resource.update(resource_id, {"status": "CONVERTING_TO_GEOJSON"})

    # Convert to GeoJSON — from DB if available and enabled, otherwise from CSV file
    if table_name:
        result = await csv_to_geojson_from_db(
            table_name,
            inspection,
            geojson_filepath,
            upload_to_minio=True,
        )
    else:
        if not file_path:
            raise ValueError("file_path is required when no parsing table name is provided")
        result = await csv_to_geojson(file_path, inspection, geojson_filepath, upload_to_minio=True)

    if result is None:
        return None
    geojson_size, geojson_url = result
    if timer:
        timer.mark("csv-to-geojson")
    if check_id is not None:
        await Check.update(
            check_id,
            {"geojson_url": geojson_url, "geojson_size": geojson_size},
        )
    return geojson_size, geojson_url


async def export_pmtiles_from_local_geojson(
    *,
    resource_id: str | None,
    check_id: int | None,
    geojson_filepath: Path,
    pmtiles_filepath: Path,
    timer: Timer | None = None,
    unlink_geojson_after: bool | None = None,
) -> tuple[int, str | None]:
    """Run tippecanoe on a local GeoJSON path, upload PMTiles, update the check."""
    if resource_id:
        # Update resource status for PMTiles conversion
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})

    # Convert GeoJSON to PMTiles
    pmtiles_size, pmtiles_url = await geojson_to_pmtiles(geojson_filepath, pmtiles_filepath)
    if timer:
        timer.mark("geojson-to-pmtiles")
    if check_id is not None:
        await Check.update(
            check_id,
            {"pmtiles_url": pmtiles_url, "pmtiles_size": pmtiles_size},
        )
    do_unlink = (
        config.REMOVE_GENERATED_FILES if unlink_geojson_after is None else unlink_geojson_after
    )
    if do_unlink and geojson_filepath.is_file():
        geojson_filepath.unlink()
    return pmtiles_size, pmtiles_url


async def csv_to_geojson_and_pmtiles(
    file_path: str,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
    timer: Timer | None = None,
    table_name: str | None = None,
) -> tuple[Path, int, str | None, Path, int, str | None] | None:
    if not config.DB_TO_GEOJSON:
        log.debug("DB_TO_GEOJSON turned off, skipping geojson/PMtiles export.")
        return None

    log.debug(
        f"Converting to geojson and PMtiles if relevant for {resource_id} and sending to MinIO."
    )

    if resource_id:
        geojson_filepath = Path(f"{resource_id}.geojson")
        pmtiles_filepath = Path(f"{resource_id}.pmtiles")
    else:
        geojson_filepath = DEFAULT_GEOJSON_FILEPATH
        pmtiles_filepath = DEFAULT_PMTILES_FILEPATH

    geo_result = await export_geojson_for_csv_analysis(
        resource_id=resource_id,
        check_id=check_id,
        inspection=inspection,
        geojson_filepath=geojson_filepath,
        file_path=file_path,
        table_name=table_name,
        timer=timer,
    )
    if geo_result is None:
        return None
    geojson_size, geojson_url = geo_result

    pmtiles_size, pmtiles_url = await export_pmtiles_from_local_geojson(
        resource_id=resource_id,
        check_id=check_id,
        geojson_filepath=geojson_filepath,
        pmtiles_filepath=pmtiles_filepath,
        timer=timer,
        unlink_geojson_after=config.REMOVE_GENERATED_FILES,
    )

    # returning only for tests purposes
    return geojson_filepath, geojson_size, geojson_url, pmtiles_filepath, pmtiles_size, pmtiles_url


async def _load_csv_inspection_for_table(resource_id: str, parsing_table: str) -> dict | None:
    db = await context.pool("csv")
    row = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1 AND parsing_table = $2 "
        "ORDER BY created_at DESC LIMIT 1",
        resource_id,
        parsing_table,
    )
    if not row:
        return None
    return json.loads(row["csv_detective"])


async def task_csv_to_geojson(
    check_id: int,
    worker_exception: bool = False,
) -> None:
    """Generate GeoJSON from the CSV parsing table and enqueue PMTiles conversion."""
    if not config.DB_TO_GEOJSON:
        log.debug("DB_TO_GEOJSON turned off, skipping geo job.")
        return

    record = await Check.get_by_id(check_id, with_deleted=True)
    if not record:
        log.error(f"task_csv_to_geojson: check {check_id} not found")
        return

    check: dict = dict(record)
    resource_id = str(check["resource_id"])
    url = check["url"]
    resource: Record | None = await Resource.get(resource_id)
    parsing_table = check.get("parsing_table")

    if not parsing_table:
        log.warning(f"task_csv_to_geojson: no parsing_table for check {check_id}")
        return

    inspection = await _load_csv_inspection_for_table(resource_id, parsing_table)
    if not inspection:
        log.error(
            f"task_csv_to_geojson: no tables_index row for resource {resource_id} table {parsing_table}",
        )
        return

    geojson_path = Path(f"{resource_id}.geojson")
    chained_pmtiles = False

    try:
        try:
            result = await export_geojson_for_csv_analysis(
                resource_id=resource_id,
                check_id=check_id,
                inspection=inspection,
                geojson_filepath=geojson_path,
                file_path=None,
                table_name=parsing_table,
                timer=None,
            )
        except Exception as e:
            remove_remainders(resource_id, ["geojson", "pmtiles", "pmtiles-journal"])
            raise ParseException(
                message=str(e),
                step="geojson_export",
                resource_id=resource_id,
                url=url,
                check_id=check_id,
            ) from e

        if result is None:
            log.debug(f"No geographical columns for {resource_id}, skipping geojson/PMTiles")
            return

        check = dict((await Check.get_by_id(check_id, with_deleted=True)) or check)

        # Update resource status for PMTiles conversion
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})
        queue.enqueue(
            task_geojson_to_pmtiles,
            check_id,
            _priority="low",
            _exception=worker_exception,
        )
        chained_pmtiles = True

        if resource is not None:
            await helpers.notify_udata(resource, check)

    except (ParseException, OSError, ValueError) as e:
        if isinstance(e, ParseException):
            await handle_parse_exception(e, parsing_table, check)
        else:
            await handle_parse_exception(
                ParseException(
                    message=str(e),
                    step="geojson_export",
                    resource_id=resource_id,
                    url=url,
                    check_id=check_id,
                ),
                parsing_table,
                check,
            )
        if resource is not None:
            refreshed = await Check.get_by_id(check_id, with_deleted=True)
            if refreshed:
                await helpers.notify_udata(resource, dict(refreshed))
    finally:
        if not chained_pmtiles:
            await Resource.update(resource_id, {"status": None})
        if config.REMOVE_GENERATED_FILES and geojson_path.is_file():
            try:
                geojson_path.unlink()
            except OSError:
                log.warning(f"Could not remove {geojson_path}")


async def task_geojson_to_pmtiles(check_id: int) -> None:
    """Convert GeoJSON (from MinIO) to PMTiles and persist URLs on the check."""
    record = await Check.get_by_id(check_id, with_deleted=True)
    if not record:
        log.error(f"task_geojson_to_pmtiles: check {check_id} not found")
        return

    check: dict = dict(record)
    resource_id = str(check["resource_id"])
    url = check["url"]
    resource: Record | None = await Resource.get(resource_id)
    geojson_url = check.get("geojson_url")

    if not geojson_url:
        log.warning(f"task_geojson_to_pmtiles: no geojson_url for check {check_id}")
        await Resource.update(resource_id, {"status": None})
        return

    # Update resource status for PMTiles conversion
    await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})
    pmtiles_path = Path(f"{resource_id}.pmtiles")
    tmp_geo: Path | None = None

    try:
        try:
            tmp_geo = await helpers.download_url_to_tempfile(geojson_url, suffix=".geojson")
            await export_pmtiles_from_local_geojson(
                resource_id=resource_id,
                check_id=check_id,
                geojson_filepath=tmp_geo,
                pmtiles_filepath=pmtiles_path,
                timer=None,
                unlink_geojson_after=False,
            )
        except Exception as e:
            remove_remainders(resource_id, ["pmtiles", "pmtiles-journal"])
            raise ParseException(
                message=str(e),
                step="pmtiles_export",
                resource_id=resource_id,
                url=url,
                check_id=check_id,
            ) from e

        refreshed = await Check.get_by_id(check_id, with_deleted=True)
        if resource is not None and refreshed:
            await helpers.notify_udata(resource, dict(refreshed))

    except (ParseException, OSError, aiohttp.ClientError) as e:
        if isinstance(e, ParseException):
            await handle_parse_exception(e, check.get("parsing_table"), check)
        else:
            await handle_parse_exception(
                ParseException(
                    message=str(e),
                    step="pmtiles_export",
                    resource_id=resource_id,
                    url=url,
                    check_id=check_id,
                ),
                check.get("parsing_table"),
                check,
            )
        if resource is not None:
            refreshed = await Check.get_by_id(check_id, with_deleted=True)
            if refreshed:
                await helpers.notify_udata(resource, dict(refreshed))
    finally:
        await Resource.update(resource_id, {"status": None})
        if tmp_geo is not None:
            try:
                tmp_geo.unlink(missing_ok=True)
            except OSError:
                log.warning(f"Could not remove temp geojson {tmp_geo}")
        if pmtiles_path.is_file() and config.REMOVE_GENERATED_FILES:
            try:
                pmtiles_path.unlink()
            except OSError:
                log.warning(f"Could not remove {pmtiles_path}")

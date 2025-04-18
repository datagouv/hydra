import logging
import os
import subprocess
from datetime import datetime, timezone

from asyncpg import Record

from udata_hydra import config
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.analysis import helpers
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
)
from udata_hydra.utils.minio import MinIOClient

log = logging.getLogger("udata-hydra")


async def analyse_geojson(
    check: dict,
    file_path: str | None = None,
) -> None:
    """Launch GeoJSON analysis from a check or an URL (debug), using previously downloaded file at file_path if any"""
    if not config.GEOJSON_ANALYSIS:
        log.debug("GEOJSON_ANALYSIS turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_GEOJSON
    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_GEOJSON"})

    # Check if the resource is in the exceptions table
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

    timer = Timer("analyse-geojson")
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

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})

        # Convert to PMTiles
        try:
            pmtiles_url = await geojson_to_pmtiles(
                file_path=tmp_file.name,
                resource_id=resource_id,
            )
            timer.mark("geojson-to-pmtiles")
        except Exception as e:
            raise ParseException(
                step="pmtiles_export", resource_id=resource_id, url=url, check_id=check["id"]
            ) from e

        check = await Check.update(
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
                "pmtiles_url": pmtiles_url,
            },
        )

    except (ParseException, IOException) as e:
        await handle_parse_exception(e, None, check)
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


async def geojson_to_pmtiles(
    file_path: str,
    resource_id: str | None = None,
) -> str:
    """
    Convert a GeoJSON file to PMTiles format.

    Args:
        file_path: GeoJSON file path to convert.
        resource_id: Optional resource ID for status updates.

    Returns:
        pmtiles_url: URL of the PMTiles file.
    """

    log.debug(f"Converting GeoJSON to PMTiles for {file_path}")

    if resource_id:
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})

    output_pmtiles = os.path.splitext(file_path)[0] + ".pmtiles"

    command = [
        "tippecanoe",
        "--maximum-zoom=g",  # guess
        "-o",
        output_pmtiles,
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        file_path,
    ]
    subprocess.run(command, check=True)
    log.debug(f"Successfully converted {file_path} to {output_pmtiles}")

    pmtiles_url: str = MinIOClient().send_file(output_pmtiles)

    return pmtiles_url

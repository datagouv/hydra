import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.conversion.geojson_to_pmtiles import geojson_to_pmtiles
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
    remove_remainders,
)

# Re-exported so that `udata_hydra.analysis.geojson.geojson_to_pmtiles` keeps
# working as a `mock.patch` target for tests that mock the call site used by
# `analyse_geojson`.
__all__ = ["analyse_geojson", "geojson_to_pmtiles"]

log = logging.getLogger("udata-hydra")


async def analyse_geojson(
    check: Record | dict,
    filename: str | None = None,
) -> None:
    """Launch GeoJSON analysis from a check or an URL (debug), using previously downloaded file if any"""
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
            filename=filename,
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

import logging
from datetime import datetime, timezone

from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.exports import export_pmtiles
from udata_hydra.data_formats import Geojson
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import Timer

# Re-exported so that `udata_hydra.analysis.geojson.geojson_to_pmtiles` keeps
# working as a `mock.patch` target for tests that mock the call site used by
# `analyse_geojson`.
__all__ = ["analyse_geojson"]

log = logging.getLogger("udata-hydra")


async def analyse_geojson(
    check: Record | dict,
    file: Geojson | None = None,
) -> None:
    """Launch GeoJSON analysis from a check or an URL (debug), using previously downloaded file if any"""
    if not config.GEOJSON_TO_PMTILES:
        log.debug("GEOJSON_TO_PMTILES turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_GEOJSON
    await Resource.update(resource_id, {"status": "ANALYSING_GEOJSON"})

    # Check if the resource is in the exceptions table
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

    timer = Timer("analyse-geojson", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    if file is None:
        tmp_file = await helpers.read_or_download_file(
            check=check,
            filename=None,
            data_format=Geojson,
            exception=exception,
        )
        file = Geojson(path=tmp_file.name, resource_id=resource_id, dataset_id=check.get("dataset_id"))
    timer.mark("download-file")

    # Convert to PMTiles
    await export_pmtiles(geojson_file=file, check=check)
    timer.mark("geojson-to-pmtiles")
    check = await Check.update(  # type: ignore[assignment]
        check_id=check["id"],
        data={
            "parsing_finished_at": datetime.now(timezone.utc),
        },
    )

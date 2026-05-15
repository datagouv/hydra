import logging
from pathlib import Path

from udata_hydra import config
from udata_hydra.conversion.csv_to_geojson import csv_to_geojson
from udata_hydra.conversion.db_to_geojson import db_to_geojson
from udata_hydra.conversion.geojson_to_pmtiles import geojson_to_pmtiles
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import Timer

DEFAULT_GEOJSON_FILEPATH = Path("converted_from_csv.geojson")
DEFAULT_PMTILES_FILEPATH = Path("converted_from_geojson.pmtiles")

log = logging.getLogger("udata-hydra")


async def csv_to_geojson_and_pmtiles(
    file_path: str,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
    timer: Timer | None = None,
    table_name: str | None = None,
) -> tuple[Path, int, str | None, Path, int, str | None] | None:
    log.debug(
        f"Converting to GeoJSON and PMTiles if relevant for {resource_id} and sending to MinIO."
    )

    if resource_id:
        geojson_filepath = Path(f"{resource_id}.geojson")
        pmtiles_filepath = Path(f"{resource_id}.pmtiles")
        # Update resource status for GeoJSON conversion
        await Resource.update(resource_id, {"status": "CONVERTING_TO_GEOJSON"})
    else:
        geojson_filepath = DEFAULT_GEOJSON_FILEPATH
        pmtiles_filepath = DEFAULT_PMTILES_FILEPATH

    # Convert to GeoJSON — from DB if available and enabled, otherwise from CSV file
    if config.DB_TO_GEOJSON and table_name:
        result = await db_to_geojson(
            table_name,
            inspection,
            geojson_filepath,
            upload_to_minio=True,
        )
    else:
        result = await csv_to_geojson(file_path, inspection, geojson_filepath, upload_to_minio=True)
    if result is None:
        return None
    geojson_size, geojson_url = result
    if timer:
        timer.mark("csv-to-geojson")

    await Check.update(
        check_id,
        {
            "geojson_url": geojson_url,
            "geojson_size": geojson_size,
        },
    )

    # Update resource status for PMTiles conversion
    if resource_id:
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})

    # Convert GeoJSON to PMTiles
    pmtiles_size, pmtiles_url = await geojson_to_pmtiles(geojson_filepath, pmtiles_filepath)
    if timer:
        timer.mark("geojson-to-pmtiles")

    await Check.update(
        check_id,
        {
            "pmtiles_url": pmtiles_url,
            "pmtiles_size": pmtiles_size,
        },
    )

    if config.REMOVE_GENERATED_FILES:
        geojson_filepath.unlink()

    # returning only for tests purposes
    return geojson_filepath, geojson_size, geojson_url, pmtiles_filepath, pmtiles_size, pmtiles_url

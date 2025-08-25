import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import tippecanoe
from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
)
from udata_hydra.utils.minio import MinIOClient

log = logging.getLogger("udata-hydra")
minio_client_pmtiles = MinIOClient(
    bucket=config.MINIO_PMTILES_BUCKET, folder=config.MINIO_PMTILES_FOLDER
)
minio_client_geojson = MinIOClient(
    bucket=config.MINIO_GEOJSON_BUCKET, folder=config.MINIO_GEOJSON_FOLDER
)


async def analyse_geojson(
    check: dict,
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

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})

        # Convert to PMTiles
        try:
            pmtiles_filepath, pmtiles_size, pmtiles_url = await geojson_to_pmtiles(
                file_path=Path(tmp_file.name),
                resource_id=resource_id,
            )
            timer.mark("geojson-to-pmtiles")
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="pmtiles_export",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e

        check = await Check.update(
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
                "pmtiles_url": pmtiles_url,
                "pmtiles_size": pmtiles_size,
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


async def csv_to_geojson(
    df: pd.DataFrame,
    inspection: dict,
    resource_id: str | None = None,
    upload_to_minio: bool = True,
) -> tuple[Path, int, str | None] | None:
    """
    Convert a CSV DataFrame to GeoJSON format and optionally upload to MinIO.

    Detects geographical columns (geometry, latlon, lonlat, or lat/lon) and converts
    CSV data to GeoJSON features. Rows with NaN values in geographical columns are skipped.

    Args:
        df: Pandas DataFrame containing the CSV data.
        inspection: CSV detective analysis results with column format detection.
        resource_id: Optional resource ID for status updates and file naming.
        upload_to_minio: Whether to upload to MinIO (default: True).

    Returns:
        geojson_filepath: GeoJSON file path.
        geojson_size: Size of the GeoJSON file in bytes.
        geojson_url: URL of the GeoJSON file on MinIO. None if it was not uploaded to MinIO.
    """

    def cast_latlon(latlon: str) -> list[float]:
        # we can safely do this as the detection was successful
        # removing potential blank and brackets
        lat, lon = latlon.replace(" ", "").replace("[", "").replace("]", "").split(",")
        # using the geojson standard: longitude before latitude
        return [float(lon), float(lat)]

    def prevent_nan(value):
        # convenience to prevent downstream crash (NaN in json or PMtiles)
        if pd.isna(value):
            return None
        return value

    log.debug(f"Converting to geojson for {resource_id}")

    geo = {}
    for column, detection in inspection["columns"].items():
        # see csv-detective's geo formats:
        # https://github.com/datagouv/csv-detective/tree/master/csv_detective/detect_fields/geo
        if "geojson" in detection["format"]:
            geo["geometry"] = column
            break
        if "latlon" in detection["format"]:
            geo["latlon"] = column
            break
        if "lonlat" in detection["format"]:
            geo["lonlat"] = column
            break
        if "latitude" in detection["format"]:
            geo["lat"] = column
        if "longitude" in detection["format"]:
            geo["lon"] = column
    # priority is given to geometry, then latlon, then latitude + longitude
    if "geometry" in geo:
        geo = {"geometry": geo["geometry"]}
    if "latlon" in geo:
        geo = {"latlon": geo["latlon"]}
    if "lonlat" in geo:
        geo = {"lonlat": geo["lonlat"]}
    if not geo or (("lat" in geo and "lon" not in geo) or ("lon" in geo and "lat" not in geo)):
        log.debug("No geographical columns found, skipping")
        return None

    if resource_id:
        await Resource.update(resource_id, {"status": "CONVERTING_TO_GEOJSON"})

    template = {"type": "FeatureCollection", "features": []}
    for _, row in df.iterrows():
        if "geometry" in geo:
            template["features"].append(
                {
                    "type": "Feature",
                    # json is not pre-cast by csv-detective
                    "geometry": json.loads(row[geo["geometry"]]),
                    "properties": {
                        col: prevent_nan(row[col]) for col in df.columns if col != geo["geometry"]
                    },
                }
            )
        elif "latlon" in geo:
            # ending up here means we either have the exact lat,lon format, or NaN
            # skipping row if NaN
            if pd.isna(row[geo["latlon"]]):
                continue
            template["features"].append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": cast_latlon(row[geo["latlon"]]),
                    },
                    "properties": {
                        col: prevent_nan(row[col]) for col in df.columns if col != geo["latlon"]
                    },
                }
            )
        elif "lonlat" in geo:
            # ending up here means we either have the exact lon,lat format, or NaN
            # skipping row if NaN
            if pd.isna(row[geo["lonlat"]]):
                continue
            template["features"].append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        # inverting lon and lat to match the standard
                        "coordinates": cast_latlon(row[geo["lonlat"]])[::-1],
                    },
                    "properties": {
                        col: prevent_nan(row[col]) for col in df.columns if col != geo["lonlat"]
                    },
                }
            )
        else:
            # skipping row if lat or lon is NaN
            if any(pd.isna(coord) for coord in (row[geo["lon"]], row[geo["lat"]])):
                continue
            template["features"].append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        # these columns are precast by csv-detective
                        "coordinates": [row[geo["lon"]], row[geo["lat"]]],
                    },
                    "properties": {
                        col: prevent_nan(row[col])
                        for col in df.columns
                        if col not in [geo["lon"], geo["lat"]]
                    },
                }
            )
    geojson_filepath = Path(f"{resource_id}.geojson")
    with open(geojson_filepath, "w") as f:
        json.dump(template, f, indent=4, ensure_ascii=False, default=str)
    geojson_size: int = os.path.getsize(geojson_filepath)

    if upload_to_minio:
        log.debug(f"Sending GeoJSON file {geojson_filepath} to MinIO")
        geojson_url = minio_client_geojson.send_file(str(geojson_filepath), delete_source=False)
    else:
        geojson_url = None

    return geojson_filepath, geojson_size, geojson_url


async def geojson_to_pmtiles(
    file_path: Path,
    resource_id: str | None = None,
    upload_to_minio: bool = True,
) -> tuple[Path, int, str | None]:
    """
    Convert a GeoJSON file to PMTiles format.

    Args:
        file_path: GeoJSON file path to convert.
        resource_id: Optional resource ID for status updates.

    Returns:
        pmtiles_filepath: PMTiles file path.
        pmtiles_size: size of the PMTiles file.
        pmtiles_url: URL of the PMTiles file on MinIO. None if it was not uploaded to MinIO.
    """

    log.debug(f"Converting GeoJSON to PMTiles for {file_path}")

    if resource_id:
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PMTILES"})

    pmtiles_filepath = Path(f"{resource_id}.pmtiles")

    command = [
        "--maximum-zoom=g",  # guess
        "-o",
        str(pmtiles_filepath),
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        str(file_path),
    ]
    exit_code = tippecanoe._program("tippecanoe", *command)
    if exit_code:
        raise ValueError(f"GeoJSON to PMTiles conversion failed with exit code {exit_code}")
    log.debug(f"Successfully converted {file_path} to {pmtiles_filepath}")

    pmtiles_size: int = os.path.getsize(pmtiles_filepath)

    if upload_to_minio:
        log.debug(f"Sending PMTiles file {pmtiles_filepath} to MinIO")
        pmtiles_url = minio_client_pmtiles.send_file(str(pmtiles_filepath), delete_source=False)
    else:
        pmtiles_url = None

    return pmtiles_filepath, pmtiles_size, pmtiles_url


async def csv_to_geojson_and_pmtiles(
    df: pd.DataFrame,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
    cleanup: bool = True,
) -> tuple[Path, int, str | None, Path, int, str | None] | None:
    if not config.CSV_TO_GEOJSON:
        log.debug("CSV_TO_GEOJSON turned off, skipping geojson/PMtiles export.")
        return None

    log.debug(
        f"Converting to geojson and PMtiles if relevant for {resource_id} and sending to MinIO."
    )

    # Convert CSV to GeoJSON
    result = await csv_to_geojson(df, inspection, resource_id, upload_to_minio=True)
    if result is None:
        return None
    geojson_filepath, geojson_size, geojson_url = result

    await Check.update(
        check_id,
        {
            "geojson_url": geojson_url,
            "geojson_size": geojson_size,
        },
    )

    # Convert GeoJSON to PMTiles
    pmtiles_filepath, pmtiles_size, pmtiles_url = await geojson_to_pmtiles(
        geojson_filepath, resource_id
    )

    await Check.update(
        check_id,
        {
            "pmtiles_url": pmtiles_url,
            "pmtiles_size": pmtiles_size,
        },
    )

    if cleanup:
        geojson_filepath.unlink()
        pmtiles_filepath.unlink()

    # returning only for tests purposes
    return geojson_filepath, geojson_size, geojson_url, pmtiles_filepath, pmtiles_size, pmtiles_url

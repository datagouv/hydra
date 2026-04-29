import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import tippecanoe
from asyncpg import Record
from json_stream import streamable_list

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.db import db_col_name
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
from udata_hydra.utils.casting import generate_records
from udata_hydra.utils.minio import MinIOClient

DEFAULT_GEOJSON_FILEPATH = Path("converted_from_csv.geojson")
DEFAULT_PMTILES_FILEPATH = Path("converted_from_geojson.pmtiles")

log = logging.getLogger("udata-hydra")


def _cast_latlon(latlon: str) -> list[float]:
    """Python version — used when reading from CSV."""
    lat, lon = latlon.replace(" ", "").replace("[", "").replace("]", "").split(",")
    # GeoJSON standard: longitude before latitude
    return [float(lon), float(lat)]


def _quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier to prevent SQL injection."""
    return '"' + name.replace('"', '""') + '"'


def _clean_pair_sql(col: str) -> str:
    """SQL version — used when reading from PostgreSQL."""
    return f"replace(replace(replace({_quote_ident(col)}, ' ', ''), '[', ''), ']', '')"


minio_client_pmtiles = MinIOClient(
    bucket=config.MINIO_PMTILES_BUCKET, folder=config.MINIO_PMTILES_FOLDER
)
minio_client_geojson = MinIOClient(
    bucket=config.MINIO_GEOJSON_BUCKET, folder=config.MINIO_GEOJSON_FOLDER
)


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


async def csv_to_geojson(
    file_path: str,
    inspection: dict,
    output_file_path: Path,
    upload_to_minio: bool = True,
) -> tuple[int, str | None] | None:
    """
    Convert a CSV file to GeoJSON format and optionally upload to MinIO.

    Detects geographical columns (geometry, latlon, lonlat, or lat/lon) and converts
    CSV data to GeoJSON features. Rows with NaN values in geographical columns are skipped.

    Args:
        file_path: Target CSV file to convert.
        inspection: CSV detective analysis results with column format detection.
        output_file_path: Path where the GeoJSON file should be saved.
        upload_to_minio: Whether to upload to MinIO (default: True).

    Returns:
        geojson_size: Size of the GeoJSON file in bytes.
        geojson_url: URL of the GeoJSON file on MinIO. None if it was not uploaded to MinIO.
    """

    def get_features(
        file_path: str, inspection: dict, geo: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        for row in generate_records(file_path, inspection, cast_json=False, as_dict=True):
            if "geojson" in geo:
                yield {
                    "type": "Feature",
                    # empty geometry cells can happen, we keep them but they won't be displayable
                    "geometry": (
                        json.loads(row[geo["geojson"]]) if row[geo["geojson"]] is not None else None
                    ),
                    "properties": {col: row[col] for col in row.keys() if col != geo["geojson"]},  # type: ignore[union-attr]
                }

            elif "latlon" in geo or "lonlat" in geo:
                pair_key = "latlon" if "latlon" in geo else "lonlat"
                pair_col = geo[pair_key]
                # skipping row if geo data is None (NaN in original CSV)
                if row[pair_col] is None:
                    continue
                coords = _cast_latlon(row[pair_col])
                # latlon already returns [lon, lat]; lonlat needs inversion
                if pair_key == "lonlat":
                    coords = coords[::-1]
                yield {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": coords,
                    },
                    "properties": {col: row[col] for col in row.keys() if col != pair_col},  # type: ignore[union-attr]
                }

            else:
                # skipping row if lat or lon is NaN
                if any(coord is None for coord in (row[geo["longitude"]], row[geo["latitude"]])):
                    continue
                yield {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        # these columns are precast by csv-detective
                        "coordinates": [row[geo["longitude"]], row[geo["latitude"]]],
                    },
                    "properties": {
                        col: row[col]
                        for col in row.keys()  # type: ignore[union-attr]
                        if col not in [geo["longitude"], geo["latitude"]]
                    },
                }

    geo = _detect_geo_columns(inspection)
    if geo is None:
        log.debug("No geographical columns found, skipping")
        return None

    template = {"type": "FeatureCollection"}

    template["features"] = streamable_list(get_features(file_path, inspection, geo))

    with output_file_path.open("w") as f:
        json.dump(template, f, indent=4, ensure_ascii=False, default=str)

    geojson_size: int = os.path.getsize(output_file_path)

    if upload_to_minio:
        log.debug(f"Sending GeoJSON file {output_file_path} to MinIO")
        geojson_url = minio_client_geojson.send_file(str(output_file_path), delete_source=False)
    else:
        geojson_url = None

    return geojson_size, geojson_url


def _detect_geo_columns(inspection: dict) -> dict[str, str] | None:
    """Detect geographical columns from csv-detective inspection results.

    Returns a dict like {"latitude": "col_name", "longitude": "col_name"}
    or {"geojson": "col_name"} etc, or None if no geo columns found.
    """
    geo = {}
    for column, detection in inspection["columns"].items():
        # see csv-detective's geo formats:
        # https://github.com/datagouv/csv-detective/tree/main/csv_detective/formats
        # geo looks like {fmt: (col, score), ...}
        for fmt in ["geojson", "latlon", "lonlat", "latitude", "longitude"]:
            # loop through the columns, for each geo format store the column that scored the highest
            if fmt in detection["format"]:
                if not geo.get(fmt):
                    geo[fmt] = (column, detection["score"])
                elif geo[fmt][1] < detection["score"]:
                    geo[fmt] = (column, detection["score"])
    # priority is given to geojson, then latlon, then lonlat, then latitude + longitude
    if "geojson" in geo:
        return {"geojson": geo["geojson"][0]}
    elif "latlon" in geo:
        return {"latlon": geo["latlon"][0]}
    elif "lonlat" in geo:
        return {"lonlat": geo["lonlat"][0]}
    elif "latitude" in geo and "longitude" in geo:
        return {"latitude": geo["latitude"][0], "longitude": geo["longitude"][0]}
    return None


def _build_feature_sql(
    table_name: str, geo: dict[str, str], columns: list[str]
) -> tuple[str, list[str]]:
    """Build a SQL query that generates GeoJSON features directly in PostgreSQL.

    Column names in `columns` and `geo` are the original CSV names. They are
    mapped to their actual DB names (handling RESERVED_COLS renaming) for the
    SQL identifiers, while the original names are passed as query parameters
    for the JSON keys so the GeoJSON output matches what the CSV path produces.

    Returns (query, params) where params are the original column names used as
    JSON keys via $1, $2… placeholders.
    """
    property_cols = [c for c in columns if c not in geo.values()]
    params: list[str] = []
    properties_fragments = []
    for idx, col in enumerate(property_cols):
        params.append(col)
        # $N::text parameters are JSON *keys* (column names), not values.
        # Values come from the quoted column identifiers and keep their native PG types.
        properties_fragments.append(f"${idx + 1}::text, {_quote_ident(db_col_name(col))}")

    # PostgreSQL's json_build_object accepts max 100 arguments (50 key-value pairs).
    # Split into chunks and merge with || when needed.
    max_pairs = 50
    chunks = [
        properties_fragments[i : i + max_pairs]
        for i in range(0, len(properties_fragments), max_pairs)
    ]
    if len(chunks) <= 1:
        properties_sql = f"json_build_object({', '.join(properties_fragments)})"
    else:
        parts = [f"jsonb_build_object({', '.join(chunk)})" for chunk in chunks]
        properties_sql = f"({' || '.join(parts)})::json"

    if "geojson" in geo:
        col = db_col_name(geo["geojson"])
        geometry_sql = f"({_quote_ident(col)})::json"
        where = ""
    elif "latlon" in geo or "lonlat" in geo:
        pair_key = "latlon" if "latlon" in geo else "lonlat"
        col = db_col_name(geo[pair_key])
        # latlon = "lat,lon" → GeoJSON needs [lon, lat] so swap indices
        # lonlat = "lon,lat" → already in GeoJSON order
        lon_idx, lat_idx = ("2", "1") if pair_key == "latlon" else ("1", "2")
        geometry_sql = f"""json_build_object(
                'type', 'Point',
                'coordinates', json_build_array(
                    (split_part({_clean_pair_sql(col)}, ',', {lon_idx}))::float,
                    (split_part({_clean_pair_sql(col)}, ',', {lat_idx}))::float
                )
            )"""
        where = f"WHERE {_quote_ident(col)} IS NOT NULL"
    else:
        lon_col = db_col_name(geo["longitude"])
        lat_col = db_col_name(geo["latitude"])
        geometry_sql = f"""json_build_object(
                'type', 'Point',
                'coordinates', json_build_array({_quote_ident(lon_col)}, {_quote_ident(lat_col)})
            )"""
        where = f"WHERE {_quote_ident(lat_col)} IS NOT NULL AND {_quote_ident(lon_col)} IS NOT NULL"

    query = f"""
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', {geometry_sql},
            'properties', {properties_sql}
        )::text AS feature_json
        FROM {_quote_ident(table_name)}
        {where}
    """
    return query, params


async def db_to_geojson(
    table_name: str,
    inspection: dict,
    output_file_path: Path,
    upload_to_minio: bool = True,
) -> tuple[int, str | None] | None:
    """Generate a GeoJSON file by streaming features directly from PostgreSQL.

    Uses a server-side cursor to avoid loading all features in memory.
    Rows with NULL geographical columns are skipped.

    Args:
        table_name: Name of the PostgreSQL table containing the CSV data.
        inspection: CSV detective analysis results with column format detection.
        output_file_path: Path where the GeoJSON file should be saved.
        upload_to_minio: Whether to upload to MinIO (default: True).

    Returns:
        geojson_size: Size of the GeoJSON file in bytes.
        geojson_url: URL of the GeoJSON file on MinIO. None if not uploaded.
    """
    geo = _detect_geo_columns(inspection)
    if geo is None:
        log.debug("No geographical columns found, skipping")
        return None

    columns = list(inspection["columns"].keys())
    query, params = _build_feature_sql(table_name, geo, columns)

    db = await context.pool("csv")
    async with db.acquire() as conn:
        async with conn.transaction():
            cursor = conn.cursor(query, *params)

            with output_file_path.open("w") as f:
                f.write('{"type": "FeatureCollection", "features": [\n')
                first = True
                async for row in cursor:
                    if not first:
                        f.write(",\n")
                    f.write(row["feature_json"])
                    first = False
                f.write("\n]}")

    geojson_size: int = os.path.getsize(output_file_path)

    if upload_to_minio:
        log.debug(f"Sending GeoJSON file {output_file_path} to MinIO")
        geojson_url = minio_client_geojson.send_file(str(output_file_path), delete_source=False)
    else:
        geojson_url = None

    return geojson_size, geojson_url


async def geojson_to_pmtiles(
    input_file_path: Path,
    output_file_path: Path,
    upload_to_minio: bool = True,
) -> tuple[int, str | None]:
    """
    Convert a GeoJSON file to PMTiles file and optionally upload to MinIO.

    Args:
        input_file_path: GeoJSON file path to convert.
        output_file_path: Path where the PMTiles file should be saved.
        upload_to_minio: Whether to upload to MinIO (default: True).

    Returns:
        pmtiles_size: size of the PMTiles file.
        pmtiles_url: URL of the PMTiles file on MinIO. None if it was not uploaded to MinIO.
    """

    log.debug(f"Converting GeoJSON file '{input_file_path}' to PMTiles file '{output_file_path}'")

    command = [
        "--maximum-zoom=g",  # guess
        "-o",
        str(output_file_path),
        "--force",  # don't crash if output file already exists, override it
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        str(input_file_path),
    ]
    exit_code = tippecanoe._program("tippecanoe", *command)
    if exit_code:
        raise ValueError(f"GeoJSON to PMTiles conversion failed with exit code {exit_code}")
    log.debug(f"Successfully converted {input_file_path} to {output_file_path}")

    pmtiles_size: int = os.path.getsize(output_file_path)

    if upload_to_minio:
        log.debug(f"Sending PMTiles file {output_file_path} to MinIO")
        pmtiles_url = minio_client_pmtiles.send_file(
            str(output_file_path), delete_source=config.REMOVE_GENERATED_FILES
        )
    else:
        pmtiles_url = None

    return pmtiles_size, pmtiles_url


async def csv_to_geojson_and_pmtiles(
    file_path: str,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
    timer: Timer | None = None,
    table_name: str | None = None,
) -> tuple[Path, int, str | None, Path, int, str | None] | None:
    if not config.CSV_TO_GEOJSON and not config.DB_TO_GEOJSON:
        log.debug("CSV_TO_GEOJSON and DB_TO_GEOJSON turned off, skipping geojson/PMtiles export.")
        return None

    log.debug(
        f"Converting to geojson and PMtiles if relevant for {resource_id} and sending to MinIO."
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

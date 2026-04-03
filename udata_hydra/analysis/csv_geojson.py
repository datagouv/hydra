import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator

from json_stream import streamable_list

from udata_hydra import config, context
from udata_hydra.db import RESERVED_COLS
from udata_hydra.utils.casting import generate_records
from udata_hydra.utils.minio import MinIOClient

DEFAULT_GEOJSON_FILEPATH = Path("converted_from_csv.geojson")

log = logging.getLogger("udata-hydra")

minio_client_geojson = MinIOClient(
    bucket=config.MINIO_GEOJSON_BUCKET, folder=config.MINIO_GEOJSON_FOLDER
)


def _cast_latlon(latlon: str) -> list[float]:
    # Detection already validated the format; values may look like "[48.8566, 2.3522]"
    # or "48.8566 , 2.3522". Strip spaces and brackets, then split on comma.
    lat, lon = latlon.replace(" ", "").replace("[", "").replace("]", "").split(",")
    # GeoJSON standard: longitude before latitude
    return [float(lon), float(lat)]


def _quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier to prevent SQL injection."""
    return '"' + name.replace('"', '""') + '"'


def _clean_pair_sql(col: str) -> str:
    """SQL version — used when reading from PostgreSQL."""
    return f"replace(replace(replace({_quote_ident(col)}, ' ', ''), '[', ''), ']', '')"


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


def _db_col_name(col: str) -> str:
    """Map a CSV column name to its actual PostgreSQL column name."""
    return f"{col}__hydra_renamed" if col.lower() in RESERVED_COLS else col


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
    for col in property_cols:
        params.append(col)
        placeholder = f"${len(params)}::text"
        properties_fragments.append(f"{placeholder}, {_quote_ident(_db_col_name(col))}")
    properties_args = ", ".join(properties_fragments)

    if "geojson" in geo:
        col = _db_col_name(geo["geojson"])
        geometry_sql = f"({_quote_ident(col)})::json"
        where = ""
    elif "latlon" in geo or "lonlat" in geo:
        pair_key = "latlon" if "latlon" in geo else "lonlat"
        col = _db_col_name(geo[pair_key])
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
        lon_col = _db_col_name(geo["longitude"])
        lat_col = _db_col_name(geo["latitude"])
        geometry_sql = f"""json_build_object(
                'type', 'Point',
                'coordinates', json_build_array({_quote_ident(lon_col)}, {_quote_ident(lat_col)})
            )"""
        where = f"WHERE {_quote_ident(lat_col)} IS NOT NULL AND {_quote_ident(lon_col)} IS NOT NULL"

    query = f"""
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', {geometry_sql},
            'properties', json_build_object({properties_args})
        )::text
        FROM {_quote_ident(table_name)}
        {where}
    """
    return query, params


async def csv_to_geojson_from_db(
    table_name: str,
    inspection: dict,
    output_file_path: Path,
    upload_to_minio: bool = True,
) -> tuple[int, str | None] | None:
    """Generate a GeoJSON file by streaming features directly from PostgreSQL."""
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
                    f.write(row[0])
                    first = False
                f.write("\n]}")

    geojson_size: int = os.path.getsize(output_file_path)

    if upload_to_minio:
        log.debug(f"Sending GeoJSON file {output_file_path} to MinIO")
        geojson_url = minio_client_geojson.send_file(str(output_file_path), delete_source=False)
    else:
        geojson_url = None

    return geojson_size, geojson_url

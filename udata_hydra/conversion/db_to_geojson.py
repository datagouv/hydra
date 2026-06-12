import logging
import os
from pathlib import Path

from udata_hydra import context
from udata_hydra.context import s3_client
from udata_hydra.conversion.csv_to_geojson import _detect_geo_columns
from udata_hydra.db import db_col_name

log = logging.getLogger("udata-hydra")


def _quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier to prevent SQL injection."""
    return '"' + name.replace('"', '""') + '"'


def _clean_pair_sql(col: str) -> str:
    """SQL version — used when reading from PostgreSQL."""
    return f"replace(replace(replace({_quote_ident(col)}, ' ', ''), '[', ''), ']', '')"


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
    upload_to_s3: bool = True,
) -> tuple[int, str | None] | None:
    """Generate a GeoJSON file by streaming features directly from PostgreSQL.

    Uses a server-side cursor to avoid loading all features in memory.
    Rows with NULL geographical columns are skipped.

    Args:
        table_name: Name of the PostgreSQL table containing the CSV data.
        inspection: CSV detective analysis results with column format detection.
        output_file_path: Path where the GeoJSON file should be saved.
        upload_to_s3: Whether to upload to S3-compatible storage (default: True).

    Returns:
        geojson_size: Size of the GeoJSON file in bytes.
        geojson_url: Public URL of the GeoJSON object. None if not uploaded.
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
                f.write('{"type": "FeatureCollection", "features": [')
                first = True
                async for row in cursor:
                    if not first:
                        f.write(",")
                    f.write(row["feature_json"])
                    first = False
                f.write("]}")

    geojson_size: int = os.path.getsize(output_file_path)

    if upload_to_s3:
        log.debug(f"Uploading GeoJSON file {output_file_path} to S3")
        geojson_url = s3_client().send_file(str(output_file_path), delete_source=False)
    else:
        geojson_url = None

    return geojson_size, geojson_url

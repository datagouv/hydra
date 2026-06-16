import logging
from typing import TYPE_CHECKING

from udata_hydra import context
from udata_hydra.data_formats import Table
from udata_hydra.data_formats.csv_like.to_geojson import (
    DEFAULT_GEOJSON_FILENAME,
    _detect_geo_columns,
)
from udata_hydra.db import db_col_name
from udata_hydra.utils import storage_path

if TYPE_CHECKING:
    from udata_hydra.data_formats import Geojson

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


async def db_to_geojson(table: Table) -> "Geojson|None":
    """Generate a GeoJSON file by streaming features directly from PostgreSQL.

    Uses a server-side cursor to avoid loading all features in memory.
    Rows with NULL geographical columns are skipped.

    Args:
        table: PostgreSQL table containing the CSV data.

    Returns:
        geojson_file: a Geojson instance.
    """
    from udata_hydra.data_formats import Geojson

    geo = _detect_geo_columns(table.inspection)
    if geo is None:
        log.debug("No geographical columns found, skipping")
        return None

    geojson_name = (
        f"{table.resource_id}.geojson"
        if table.resource_id is not None
        else DEFAULT_GEOJSON_FILENAME
    )
    log.debug(f"Converting db table '{table.table_name}' to Geojson file '{geojson_name}'")

    columns = list(table.inspection["columns"].keys())
    query, params = _build_feature_sql(table.table_name, geo, columns)

    db = await context.pool("csv")
    async with db.acquire() as conn:
        async with conn.transaction():
            cursor = conn.cursor(query, *params)

            with open(storage_path(geojson_name), "w") as f:
                f.write('{"type": "FeatureCollection", "features": [')
                first = True
                async for row in cursor:
                    if not first:
                        f.write(",")
                    f.write(row["feature_json"])
                    first = False
                f.write("]}")

    return Geojson(
        file_name=geojson_name,
        inspection=table.inspection,
        resource_id=table.resource_id,
        dataset_id=table.dataset_id,
    )

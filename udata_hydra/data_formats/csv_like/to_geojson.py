import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from json_stream import streamable_list

from udata_hydra.data_formats import CsvLike
from udata_hydra.utils.casting import iter_tabular_rows

if TYPE_CHECKING:
    from udata_hydra.data_formats import Geojson

log = logging.getLogger("udata-hydra")
DEFAULT_GEOJSON_FILENAME = "converted_from_db.geojson"


def _cast_latlon(latlon: str) -> list[float]:
    """Python version — used when reading from CSV."""
    lat, lon = latlon.replace(" ", "").replace("[", "").replace("]", "").split(",")
    # GeoJSON standard: longitude before latitude
    return [float(lon), float(lat)]


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


async def csv_to_geojson(file: CsvLike) -> "Geojson|None":
    """
    Convert a CSV file to GeoJSON format and optionally upload to S3-compatible storage.

    Detects geographical columns (geometry, latlon, lonlat, or lat/lon) and converts
    CSV data to GeoJSON features. Rows with NaN values in geographical columns are skipped.

    Args:
        file: a CsvLike instance.

    Returns:
        geojson_file: a Geojson instance.
    """

    def get_features(
        file_path: Path, inspection: dict, geo: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        for row in iter_tabular_rows(file_path, inspection, cast_json=False, as_dict=True):
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

    from udata_hydra.data_formats import Geojson

    geo = _detect_geo_columns(file.inspection)
    if geo is None:
        log.debug("No geographical columns found, skipping")
        return None

    template = {"type": "FeatureCollection"}

    template["features"] = streamable_list(get_features(file.path, file.inspection, geo))

    geojson_path = Path(
        f"{file.resource_id}.geojson"
        if file.resource_id is not None
        else DEFAULT_GEOJSON_FILENAME
    )

    with geojson_path.open("w") as f:
        json.dump(template, f, indent=4, ensure_ascii=False, default=str)

    return Geojson(
        path=geojson_path, inspection=file.inspection, resource_id=file.resource_id, dataset_id=file.dataset_id,
    )

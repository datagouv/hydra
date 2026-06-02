import json
import os
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast

import pytest
from csv_detective import routine as csv_detective_routine

from tests.conftest import RESOURCE_ID
from udata_hydra.conversion.csv_to_geojson import csv_to_geojson


def _build_csv_content(columns: dict, sep: str = ";") -> str:
    row_count = len(next(iter(columns.values())))
    content = sep.join(columns) + "\n"
    for i in range(row_count):
        content += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"
    return content


async def _geojson_from_csv_columns(
    *,
    columns: dict,
    inspection_check: Callable[[dict], None] | None = None,
    sep: str = ";",
) -> tuple[dict, tuple[int, str | None]]:
    """Build CSV from columns, convert to GeoJSON, return (geojson, csv_to_geojson result)."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    with NamedTemporaryFile(delete=False, suffix=".csv") as fp:
        fp.write(_build_csv_content(columns, sep).encode("utf-8"))
        fp.seek(0)
        csv_path = fp.name
        inspection = cast(
            dict,
            csv_detective_routine(
                file_path=csv_path,
                output_profile=True,
                num_rows=-1,
                save_results=False,
            ),
        )
        if inspection_check is not None:
            inspection_check(inspection)

        result = await csv_to_geojson(csv_path, inspection, output_path, upload_to_s3=False)

    assert result is not None
    with open(output_path) as f:
        geojson: dict = json.load(f)

    output_path.unlink()
    os.unlink(csv_path)
    return geojson, result


def _assert_lonlat_format(inspection: dict) -> None:
    assert "lonlat" in inspection["columns"]["geopoint"]["format"]


def _assert_geojson_column_format(inspection: dict) -> None:
    assert "geojson" in inspection["columns"]["polyg"]["format"]


@pytest.mark.parametrize(
    "geo_columns",
    (
        {"coords": [f"{10.0 * k * (-1) ** k},{20.0 * k * (-1) ** k}" for k in range(1, 6)]},
        {
            "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
            "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
        },
    ),
)
@pytest.mark.asyncio
async def test_csv_to_geojson(geo_columns):
    expected_lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    expected_lons = [20.0 * k * (-1) ** k for k in range(1, 6)]
    other_columns = {
        "nombre": range(1, 6),
        "score": [0.01, 1.2, 34.5, 678.9, 10],
    }
    geojson, result = await _geojson_from_csv_columns(columns=other_columns | geo_columns)
    geojson_size, geojson_url = result
    assert geojson_url is None
    assert geojson_size > 0

    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        assert feat["type"] == "Feature"
        assert feat["geometry"]["type"] == "Point"
        coords = feat["geometry"]["coordinates"]
        assert coords[0] == pytest.approx(expected_lons[i])
        assert coords[1] == pytest.approx(expected_lats[i])
        assert "nombre" in feat["properties"]
        assert "score" in feat["properties"]
        for geo_col in geo_columns:
            assert geo_col not in feat["properties"]


@pytest.mark.asyncio
async def test_csv_to_geojson_lonlat():
    """lonlat format ("[lon, lat]") should produce correct GeoJSON coordinates [lon, lat]."""
    lons = [20.0 * k * (-1) ** k for k in range(1, 6)]
    lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    geojson, _ = await _geojson_from_csv_columns(
        columns={
            "nombre": range(1, 6),
            "geopoint": [f"[{lon}, {lat}]" for lon, lat in zip(lons, lats)],
        },
        inspection_check=_assert_lonlat_format,
    )

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        coords = feat["geometry"]["coordinates"]
        assert coords[0] == pytest.approx(lons[i])
        assert coords[1] == pytest.approx(lats[i])
        assert "geopoint" not in feat["properties"]
        assert "nombre" in feat["properties"]


@pytest.mark.asyncio
async def test_csv_to_geojson_geojson_column():
    """A column containing GeoJSON strings should produce valid geometry from CSV."""
    geometries = [
        {"type": "Point", "coordinates": [10 * k * (-1) ** k, 20 * k * (-1) ** k]}
        for k in range(1, 6)
    ]
    geojson, _ = await _geojson_from_csv_columns(
        columns={
            "nombre": range(1, 6),
            "polyg": [json.dumps(g) for g in geometries],
        },
        inspection_check=_assert_geojson_column_format,
    )

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        assert feat["geometry"] == geometries[i]
        assert "polyg" not in feat["properties"]
        assert "nombre" in feat["properties"]


@pytest.mark.asyncio
async def test_csv_to_geojson_skips_rows_with_missing_latlon():
    """Rows with empty latlon values should be skipped."""
    geojson, _ = await _geojson_from_csv_columns(
        columns={
            "nombre": range(1, 4),
            "coords": ["1.0,2.0", "", "3.0,4.0"],
        },
    )

    assert len(geojson["features"]) == 2
    assert geojson["features"][0]["geometry"]["coordinates"] == pytest.approx([2.0, 1.0])
    assert geojson["features"][1]["geometry"]["coordinates"] == pytest.approx([4.0, 3.0])


@pytest.mark.asyncio
async def test_csv_to_geojson_skips_rows_with_missing_latitude_longitude():
    """Rows with empty latitude or longitude should be skipped."""
    geojson, _ = await _geojson_from_csv_columns(
        columns={
            "nombre": range(1, 4),
            "lat": [1.0, "", 3.0],
            "long": [2.0, 4.0, 6.0],
        },
    )

    assert len(geojson["features"]) == 2
    assert geojson["features"][0]["geometry"]["coordinates"] == pytest.approx([2.0, 1.0])
    assert geojson["features"][1]["geometry"]["coordinates"] == pytest.approx([6.0, 3.0])


@pytest.mark.asyncio
async def test_csv_to_geojson_returns_none_without_geo_columns():
    """Return None when csv-detective finds no geographical columns."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    with NamedTemporaryFile(delete=False, suffix=".csv") as fp:
        fp.write(b"nombre;score\n1;0.5\n2;1.0\n")
        csv_path = fp.name

    inspection = cast(
        dict,
        csv_detective_routine(
            file_path=csv_path,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        ),
    )

    result = await csv_to_geojson(csv_path, inspection, output_path, upload_to_s3=False)

    assert result is None
    assert not output_path.exists()
    os.unlink(csv_path)

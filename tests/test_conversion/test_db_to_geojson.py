import json
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Csv
from udata_hydra.data_formats.csv_like.to_geojson import _cast_latlon, _detect_geo_columns


def _build_csv_content(columns: dict, sep: str = ";") -> str:
    row_count = len(next(iter(columns.values())))
    content = sep.join(columns) + "\n"
    for i in range(row_count):
        content += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"
    return content


async def _geojson_from_columns(
    db,
    *,
    columns: dict,
    table_name: str,
    inspection_check: Callable[[dict], None] | None = None,
    sep: str = ";",
    fake_check,
) -> dict:
    """Build CSV from columns, convert to DB + GeoJSON, return (geojson, db_to_geojson result)."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    check = await fake_check()
    with NamedTemporaryFile(delete=False) as fp:
        fp.write(_build_csv_content(columns, sep).encode("utf-8"))
        fp.seek(0)
        file = Csv(path=fp.name, resource_id=RESOURCE_ID)
        inspection = await file.inspect()
        if inspection_check is not None:
            inspection_check(inspection)

        table = await file.to_db(check=check)
        geojson_file = await table.to_geojson()
        assert geojson_file is not None
    with open(output_path) as f:
        geojson: dict = json.load(f)
    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    return geojson


def _assert_lonlat_format(inspection: dict) -> None:
    assert "lonlat" in inspection["columns"]["geopoint"]["format"]


def _assert_geojson_column_format(inspection: dict) -> None:
    assert "geojson" in inspection["columns"]["polyg"]["format"]


@pytest.mark.parametrize(
    "geo_columns",
    (
        # latlon format: "lat,lon" → GeoJSON coordinates should be [lon, lat]
        {"coords": [f"{10.0 * k * (-1) ** k},{20.0 * k * (-1) ** k}" for k in range(1, 6)]},
        # separate latitude + longitude columns
        {
            "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
            "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
        },
    ),
)
@pytest.mark.asyncio
async def test_db_to_geojson(db, geo_columns, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    expected_lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    expected_lons = [20.0 * k * (-1) ** k for k in range(1, 6)]
    other_columns = {
        "nombre": range(1, 6),
        "score": [0.01, 1.2, 34.5, 678.9, 10],
    }
    geojson = await _geojson_from_columns(
        db,
        columns=other_columns | geo_columns,
        table_name="test_geojson_from_db",
        fake_check=fake_check,
    )

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
async def test_db_to_geojson_with_reserved_column(db, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    """A CSV with a reserved PG column name (xmin) should still produce valid GeoJSON from DB."""
    geojson = await _geojson_from_columns(
        db,
        columns={
            "xmin": range(1, 6),
            "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
            "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
        },
        table_name="test_geojson_reserved_col",
        fake_check=fake_check,
    )

    assert len(geojson["features"]) == 5
    expected_xmin_values = list(range(1, 6))
    actual_xmin_values = [feat["properties"]["xmin"] for feat in geojson["features"]]
    assert actual_xmin_values == expected_xmin_values


@pytest.mark.asyncio
async def test_db_to_geojson_with_quote_in_column_name(db, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    """A CSV with a single quote in a column name should not break the SQL query."""
    geojson = await _geojson_from_columns(
        db,
        columns={
            "l'adresse": [
                "10 rue de la Paix",
                "5 avenue Foch",
                "3 bd Raspail",
                "1 place Vendôme",
                "8 rue Rivoli",
            ],
            "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
            "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
        },
        table_name="test_geojson_quote_col",
        fake_check=fake_check,
    )

    assert len(geojson["features"]) == 5
    for feat in geojson["features"]:
        assert "l'adresse" in feat["properties"]


@pytest.mark.asyncio
async def test_db_to_geojson_lonlat(db, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    """lonlat format ("[lon, lat]") should produce correct GeoJSON coordinates [lon, lat]."""
    lons = [20.0 * k * (-1) ** k for k in range(1, 6)]
    lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    geojson = await _geojson_from_columns(
        db,
        columns={
            "nombre": range(1, 6),
            "geopoint": [f"[{lon}, {lat}]" for lon, lat in zip(lons, lats)],
        },
        table_name="test_geojson_lonlat",
        inspection_check=_assert_lonlat_format,
        fake_check=fake_check,
    )

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        coords = feat["geometry"]["coordinates"]
        assert coords[0] == pytest.approx(lons[i])
        assert coords[1] == pytest.approx(lats[i])
        assert "geopoint" not in feat["properties"]
        assert "nombre" in feat["properties"]


@pytest.mark.asyncio
async def test_db_to_geojson_geojson_column(db, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    """A column containing GeoJSON strings should produce valid geometry from DB."""
    geometries = [
        {"type": "Point", "coordinates": [10 * k * (-1) ** k, 20 * k * (-1) ** k]}
        for k in range(1, 6)
    ]
    geojson = await _geojson_from_columns(
        db,
        columns={
            "nombre": range(1, 6),
            "polyg": [json.dumps(g) for g in geometries],
        },
        table_name="test_geojson_geojson_col",
        inspection_check=_assert_geojson_column_format,
        fake_check=fake_check,
    )

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        assert feat["geometry"] == geometries[i]
        assert "polyg" not in feat["properties"]
        assert "nombre" in feat["properties"]


@pytest.mark.asyncio
async def test_db_to_geojson_many_columns(db, clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_GEOJSON", True)
    """More than 50 property columns should trigger json_build_object chunking."""
    columns = {f"col_{i:03d}": range(1, 6) for i in range(55)}
    columns["lat"] = [10.0 * k * (-1) ** k for k in range(1, 6)]
    columns["long"] = [20.0 * k * (-1) ** k for k in range(1, 6)]
    geojson = await _geojson_from_columns(
        db,
        columns=columns,
        table_name="test_geojson_many_cols",
        fake_check=fake_check,
    )

    assert len(geojson["features"]) == 5
    feat = geojson["features"][0]
    for i in range(55):
        assert f"col_{i:03d}" in feat["properties"]
    assert "lat" not in feat["properties"]
    assert "long" not in feat["properties"]


def _geo_col(formats: dict[str, float], score: float = 1.0) -> dict:
    return {"format": formats, "score": score}


@pytest.mark.parametrize(
    "inspection,expected",
    (
        (
            {"columns": {"geom": _geo_col({"geojson": 1.0}), "coords": _geo_col({"latlon": 1.0})}},
            {"geojson": "geom"},
        ),
        (
            {"columns": {"coords": _geo_col({"latlon": 1.0})}},
            {"latlon": "coords"},
        ),
        (
            {"columns": {"geopoint": _geo_col({"lonlat": 1.0})}},
            {"lonlat": "geopoint"},
        ),
        (
            {
                "columns": {
                    "lat": _geo_col({"latitude": 1.0}),
                    "long": _geo_col({"longitude": 1.0}),
                }
            },
            {"latitude": "lat", "longitude": "long"},
        ),
        (
            {"columns": {"lat": _geo_col({"latitude": 1.0})}},
            None,
        ),
        (
            {"columns": {"nombre": _geo_col({}), "score": _geo_col({})}},
            None,
        ),
        (
            {
                "columns": {
                    "low": _geo_col({"latlon": 1.0}, score=0.3),
                    "high": _geo_col({"latlon": 1.0}, score=0.9),
                }
            },
            {"latlon": "high"},
        ),
    ),
)
def test_detect_geo_columns(inspection: dict, expected: dict[str, str] | None) -> None:
    """Pick geo columns from csv-detective inspection with correct format priority."""
    assert _detect_geo_columns(inspection) == expected


@pytest.mark.parametrize(
    "latlon,expected",
    (
        ("1.5,2.5", [2.5, 1.5]),
        ("[1.5, 2.5]", [2.5, 1.5]),
    ),
)
def test_cast_latlon(latlon: str, expected: list[float]) -> None:
    """Convert lat,lon strings to GeoJSON [lon, lat] coordinate order."""
    assert _cast_latlon(latlon) == expected

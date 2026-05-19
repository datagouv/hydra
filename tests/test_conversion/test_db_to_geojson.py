import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from csv_detective import routine as csv_detective_routine

from tests.conftest import RESOURCE_ID
from udata_hydra.conversion.csv_to_db import csv_to_db
from udata_hydra.conversion.db_to_geojson import db_to_geojson

pytestmark = pytest.mark.asyncio


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
async def test_db_to_geojson(db, geo_columns, clean_db):
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    expected_lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    expected_lons = [20.0 * k * (-1) ** k for k in range(1, 6)]

    other_columns = {
        "nombre": range(1, 6),
        "score": [0.01, 1.2, 34.5, 678.9, 10],
    }
    sep = ";"
    columns = other_columns | geo_columns
    file = sep.join(columns) + "\n"
    for i in range(len(other_columns["nombre"])):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    table_name = "test_geojson_from_db"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None
    geojson_size, geojson_url = result
    assert geojson_url is None
    assert geojson_size > 0

    with open(output_path) as f:
        geojson = json.load(f)

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

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_db_to_geojson_with_reserved_column(db, clean_db):
    """A CSV with a reserved PG column name (xmin) should still produce valid GeoJSON from DB."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    sep = ";"
    columns = {
        "xmin": range(1, 6),
        "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
        "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
    }
    file = sep.join(columns) + "\n"
    for i in range(5):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    table_name = "test_geojson_reserved_col"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None
    geojson_size, _ = result

    with open(output_path) as f:
        geojson = json.load(f)

    assert len(geojson["features"]) == 5
    expected_xmin_values = list(range(1, 6))
    actual_xmin_values = [feat["properties"]["xmin"] for feat in geojson["features"]]
    assert actual_xmin_values == expected_xmin_values

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_db_to_geojson_with_quote_in_column_name(db, clean_db):
    """A CSV with a single quote in a column name should not break the SQL query."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    sep = ";"
    columns = {
        "l'adresse": [
            "10 rue de la Paix",
            "5 avenue Foch",
            "3 bd Raspail",
            "1 place Vendôme",
            "8 rue Rivoli",
        ],
        "lat": [10.0 * k * (-1) ** k for k in range(1, 6)],
        "long": [20.0 * k * (-1) ** k for k in range(1, 6)],
    }
    file = sep.join(columns) + "\n"
    for i in range(5):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    table_name = "test_geojson_quote_col"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None

    with open(output_path) as f:
        geojson = json.load(f)

    assert len(geojson["features"]) == 5
    for feat in geojson["features"]:
        assert "l'adresse" in feat["properties"]

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_db_to_geojson_lonlat(db, clean_db):
    """lonlat format ("[lon, lat]") should produce correct GeoJSON coordinates [lon, lat]."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    lons = [20.0 * k * (-1) ** k for k in range(1, 6)]
    lats = [10.0 * k * (-1) ** k for k in range(1, 6)]
    sep = ";"
    columns = {
        "nombre": range(1, 6),
        "geopoint": [f"[{lon}, {lat}]" for lon, lat in zip(lons, lats)],
    }
    file = sep.join(columns) + "\n"
    for i in range(5):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    assert "lonlat" in inspection["columns"]["geopoint"]["format"]

    table_name = "test_geojson_lonlat"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None

    with open(output_path) as f:
        geojson = json.load(f)

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        coords = feat["geometry"]["coordinates"]
        assert coords[0] == pytest.approx(lons[i])
        assert coords[1] == pytest.approx(lats[i])
        assert "geopoint" not in feat["properties"]
        assert "nombre" in feat["properties"]

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_db_to_geojson_geojson_column(db, clean_db):
    """A column containing GeoJSON strings should produce valid geometry from DB."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    geometries = [
        {"type": "Point", "coordinates": [10 * k * (-1) ** k, 20 * k * (-1) ** k]}
        for k in range(1, 6)
    ]
    sep = ";"
    columns = {
        "nombre": range(1, 6),
        "polyg": [json.dumps(g) for g in geometries],
    }
    file = sep.join(columns) + "\n"
    for i in range(5):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    assert "geojson" in inspection["columns"]["polyg"]["format"]

    table_name = "test_geojson_geojson_col"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None

    with open(output_path) as f:
        geojson = json.load(f)

    assert len(geojson["features"]) == 5
    for i, feat in enumerate(geojson["features"]):
        assert feat["geometry"] == geometries[i]
        assert "polyg" not in feat["properties"]
        assert "nombre" in feat["properties"]

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_db_to_geojson_many_columns(db, clean_db):
    """More than 50 property columns should trigger json_build_object chunking."""
    output_path = Path(f"{RESOURCE_ID}.geojson")
    try:
        output_path.unlink()
    except FileNotFoundError:
        pass

    sep = ";"
    columns = {f"col_{i:03d}": range(1, 6) for i in range(55)}
    columns["lat"] = [10.0 * k * (-1) ** k for k in range(1, 6)]
    columns["long"] = [20.0 * k * (-1) ** k for k in range(1, 6)]
    file = sep.join(columns) + "\n"
    for i in range(5):
        file += sep.join(str(val) for val in [data[i] for data in columns.values()]) + "\n"

    with NamedTemporaryFile(delete=False) as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )

    table_name = "test_geojson_many_cols"
    await csv_to_db(fp.name, inspection, table_name)

    result = await db_to_geojson(table_name, inspection, output_path, upload_to_minio=False)
    assert result is not None

    with open(output_path) as f:
        geojson = json.load(f)

    assert len(geojson["features"]) == 5
    feat = geojson["features"][0]
    for i in range(55):
        assert f"col_{i:03d}" in feat["properties"]
    assert "lat" not in feat["properties"]
    assert "long" not in feat["properties"]

    output_path.unlink()
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')

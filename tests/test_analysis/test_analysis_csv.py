import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import ClientSession
from asyncpg.exceptions import UndefinedTableError
from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.conversion.csv_to_db import csv_to_db
from udata_hydra.conversion.db_to_geojson import db_to_geojson
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("debug_insert", [True, False])
async def test_analyse_csv_on_catalog(
    setup_catalog, rmock, catalog_content, db, debug_insert, fake_check, produce_mock
):
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body=catalog_content)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None
    assert resource["status_since"] is None

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None
    assert isinstance(resource["status_since"], datetime)

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] == table_name
    assert res["parsing_error"] is None
    rows = list(await db.fetch(f'SELECT * FROM "{table_name}"'))
    assert len(rows) == 2
    row = rows[0]
    assert row["id"] == RESOURCE_ID
    assert row["url"] == RESOURCE_URL
    res = await db.fetchrow("SELECT * from tables_index")
    inspection = json.loads(res["csv_detective"])
    assert all(k in inspection["columns"] for k in ["id", "url"])


@pytest.mark.slow
async def test_analyse_csv_big_file(setup_catalog, rmock, db, fake_check, produce_mock):
    """
    This test is slow because it parses a pretty big file.
    It's meant to act as a "canary in the coal mine": if performance degrades too much, you or the CI should feel it.
    You can deselect it by running `pytest -m "not slow"`.
    """
    TEST_CSV_FILE, EXPECTED_COUNT = ("20190618-annuaire-diagnostiqueurs.csv", 45522)

    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()

    csv_path = "tests/data" / Path(TEST_CSV_FILE)
    with csv_path.open("rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == EXPECTED_COUNT
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == EXPECTED_COUNT


@pytest.mark.parametrize(
    "line_expected",
    (
        # (int, float, string, bool), (__id, int, float, string, bool)
        ("1,1020.20,test,true", (1, 1, 1020.2, "test", True), ","),
        ('2,"1020,20",test,false', (1, 2, 1020.2, "test", False), ","),
        ("1;1020.20;test;true", (1, 1, 1020.2, "test", True), ";"),
        ("2;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0|1020,20|test|false", (1, 2, 1020.2, "test", False), "|"),
    ),
)
async def test_csv_to_db_simple_type_casting(db, line_expected, clean_db):
    line, expected, separator = line_expected
    header = separator.join(["int", "float", "string", "bool"])
    with NamedTemporaryFile() as fp:
        fp.write(f"{header}\n{line}".encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            num_rows=-1,
            save_results=False,
        )
        assert inspection["separator"] == separator
        await csv_to_db(fp.name, inspection=inspection, table_name="test_table")
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "int", "float", "string", "bool"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


@pytest.mark.parametrize(
    "line_expected",
    (
        # (json, date, datetime, aware_datetime), (__id, json, date, datetime, aware_datetime)
        (
            '{"a": 1};31 décembre 2022;2022-31-12 12:00:00.92;2030-06-22 00:00:00.0028+02:00',
            (
                1,
                json.dumps({"a": 1}),
                date(2022, 12, 31),
                datetime(2022, 12, 31, 12, 0, 0, 920000),
                datetime(2030, 6, 22, 0, 0, 0, 2800, tzinfo=timezone(timedelta(seconds=7200))),
            ),
        ),
        (
            '[{"a": 1, "b": 2}];31st december 2022;12/31/2022 12:00:00;1996/06/22 10:20:10 GMT',
            (
                1,
                json.dumps([{"a": 1, "b": 2}]),
                date(2022, 12, 31),
                datetime(2022, 12, 31, 12, 0, 0),
                datetime(1996, 6, 22, 10, 20, 10, tzinfo=timezone.utc),
            ),
        ),
    ),
)
async def test_csv_to_db_complex_type_casting(db, line_expected, clean_db):
    line, expected = line_expected
    with NamedTemporaryFile() as fp:
        fp.write(f"json;date;datetime;aware_datetime\n{line}".encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            encoding="utf-8",
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(fp.name, inspection=inspection, table_name="test_table", debug_insert=True)
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "json", "date", "datetime", "aware_datetime"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


async def test_basic_sql_injection(db, clean_db):
    # tries to execute
    # CREATE TABLE table_name("int" integer, "col_name" text);DROP TABLE toto;--)
    injection = 'col_name" text);DROP TABLE toto;--'
    with NamedTemporaryFile() as fp:
        fp.write(f"int,{injection}\n1,test".encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            sep=",",
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(fp.name, inspection=inspection, table_name="test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res[injection] == "test"


async def test_percentage_column(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int,% mon pourcent\n1,test".encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(fp.name, inspection=inspection, table_name="test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["% mon pourcent"] == "test"


async def test_reserved_column_name(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int,xmin\n1,test".encode("utf-8"))
        fp.seek(0)
        inspection = csv_detective_routine(
            file_path=fp.name,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(fp.name, inspection=inspection, table_name="test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["xmin__hydra_renamed"] == "test"


async def test_error_reporting_csv_detective(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body="".encode("utf-8"))

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert res["parsing_error"] == "csv_detective:Could not accurately retrieve headers position"
    assert res["parsing_finished_at"]


async def test_error_reporting_parsing(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body="a,b,c\n1,2".encode("utf-8"))

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert (
        res["parsing_error"]
        == "csv_detective:Number of columns is not even across the first 10 rows (detected separator: ,)."
    )
    assert res["parsing_finished_at"]
    with pytest.raises(UndefinedTableError):
        await db.execute(f'SELECT * FROM "{table_name}"')


async def test_analyse_csv_send_udata_webhook(
    setup_catalog, rmock, catalog_content, db, fake_check, udata_url
):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    rmock.put(udata_url, status=200)

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] is None

    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("analysis:parsing:started_at")
    assert webhook.get("analysis:parsing:finished_at")
    assert webhook.get("analysis:parsing:parsing_table")
    assert webhook.get("analysis:parsing:error") is None
    for k in ["parquet_size", "parquet_url"]:
        assert webhook.get(f"analysis:parsing:{k}", False) is None


@pytest.mark.parametrize(
    "forced_analysis",
    (
        (True, True),
        (False, False),
    ),
)
async def test_forced_analysis(
    setup_catalog,
    rmock,
    catalog_content,
    db,
    fake_check,
    forced_analysis,
    udata_url,
):
    force_analysis, table_exists = forced_analysis
    check = await fake_check(
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        detected_last_modified_at=datetime(2025, 4, 4, 0, 0, 0),
    )
    url = check["url"]
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    rmock.head(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
    )
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body="a,b,c\n1,2,3".encode("utf-8"),
        repeat=True,
    )
    rmock.put(udata_url, status=200, repeat=True)
    async with ClientSession() as session:
        await check_resource(
            url=url, resource=resource, session=session, force_analysis=force_analysis
        )

    # check that csv was indeed pushed to db
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert (table_name in [r["table_name"] for r in tables]) == table_exists

    # check whether udata was pinged
    if force_analysis:
        webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
        assert webhook.get("analysis:parsing:started_at")
        assert webhook.get("analysis:parsing:finished_at")
        assert webhook.get("analysis:parsing:parsing_table")
        assert webhook.get("analysis:parsing:error") is None
        for k in ["parquet_size", "parquet_url"]:
            assert webhook.get(f"analysis:parsing:{k}", False) is None
    else:
        assert ("PUT", URL(udata_url)) not in rmock.requests.keys()


def create_body(
    separator: str,
    header: list[str],
    rows: list[list[str]],
    encoding: str,
    **kwargs,
) -> bytes:
    return "\n".join(separator.join(cell for cell in row) for row in [header] + rows).encode(
        encoding
    )


default_kwargs = {
    "separator": ",",
    "header": ["a", "epci"],
    "rows": [["1", "13002526500013"], ["5", "38271817900023"]],
    "encoding": "utf-8",
    "columns": {
        "a": {"score": 1.0, "format": "int", "python_type": "int"},
        "epci": {"score": 1.5, "format": "siret", "python_type": "string"},
    },
    "header_row_idx": 0,
    "categorical": None,
    "formats": {"int": ["a"], "siret": ["epci"]},
    "columns_fields": None,
    "columns_labels": None,
    "profile": None,
}

python_type_to_sql = {
    "int": "INT",
    "string": "VARCHAR",
    "float": "FLOAT",
}


def create_analysis(scan: dict) -> dict:
    analysis = {
        k: scan[k]
        for k in [
            "encoding",
            "separator",
            "header_row_idx",
            "header",
            "columns",
            "categorical",
            "formats",
            "columns_fields",
            "columns_labels",
            "profile",
        ]
    }
    analysis["total_lines"] = len(scan["rows"])
    return analysis


@pytest.mark.parametrize(
    "_params",
    (
        # just a new row in the file, types haven't changed
        (
            default_kwargs,
            default_kwargs | {"rows": default_kwargs["rows"] + [["6", "21310555400017"]]},
            True,
        ),
        # separator changed
        (default_kwargs, default_kwargs | {"separator": ";"}, False),
        # columns changed
        (
            default_kwargs,
            default_kwargs
            | {
                "header": ["a", "ID_EPCI"],
                "columns": {
                    "a": {"score": 1.0, "format": "int", "python_type": "int"},
                    "ID_EPCI": {"score": 1.25, "format": "siret", "python_type": "string"},
                },
                "formats": {"int": ["a"], "siret": ["ID_EPCI"]},
            },
            False,
        ),
        # format changed
        (
            default_kwargs,
            default_kwargs
            | {
                "header": ["a", "b"],
                "rows": [["1", "2022-11-03"], ["5", "2025-11-02"]],
                "columns": {
                    "a": {"score": 1.0, "format": "int", "python_type": "int"},
                    "b": {"score": 1.0, "format": "date", "python_type": "date"},
                },
                "formats": {"int": ["a"], "date": ["b"]},
            },
            False,
        ),
    ),
)
async def test_validation(
    setup_catalog,
    rmock,
    db,
    fake_check,
    udata_url,
    _params,
    produce_mock,
):
    previous_scan_kwargs, current_scan_kwargs, is_valid = _params
    previous_analysis = create_analysis(previous_scan_kwargs)
    current_analysis = create_analysis(current_scan_kwargs)

    # set up check and url
    check = await fake_check(
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
    )
    url = check["url"]
    rmock.get(
        url,
        status=200,
        body=create_body(**current_scan_kwargs),
    )
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()

    # set up previous analysis and db insertion
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) VALUES($1, $2, $3, $4, $5)",
        table_name,
        json.dumps(previous_analysis),
        check.get("resource_id"),
        check.get("dataset_id"),
        check.get("url"),
    )
    await db.execute(
        f'CREATE TABLE "{table_name}"(__id serial PRIMARY KEY, '
        + ", ".join(
            f"{col} {python_type_to_sql[previous_analysis['columns'][col]['python_type']]}"
            for col in previous_analysis["columns"]
        )
        + ")"
    )
    await db.execute(
        f'INSERT INTO "{table_name}" ({", ".join(previous_analysis["header"])}) VALUES '
        + ", ".join(f"({','.join(row)})" for row in previous_scan_kwargs["rows"])
    )

    # run analysis
    with patch(
        # wraps because we don't want to change the behaviour, just to know it's been called
        "udata_hydra.analysis.csv.validate_then_detect",
        wraps=validate_then_detect,
    ) as mock_func:
        await analyse_csv(check=check)
        mock_func.assert_called_once()

    # now we check what is inside csv_detective in tables_index
    res = await db.fetch(
        f"SELECT * from tables_index WHERE resource_id='{check['resource_id']}' "
        "ORDER BY created_at DESC"
    )
    assert len(res) == 2
    assert all(row["parsing_table"] == table_name for row in res)
    assert all(str(row["resource_id"]) == check["resource_id"] for row in res)
    assert all(row["url"] == check["url"] for row in res)
    assert all(row["dataset_id"] == check["dataset_id"] for row in res)
    assert all(row["deleted_at"] is None for row in res)  # Should be NULL for new records
    latest_analysis = json.loads(res[0]["csv_detective"])
    # check that latest analysis is in line with expectation
    for key in current_analysis.keys():
        if current_analysis[key] is not None:
            assert latest_analysis[key] == current_analysis[key]
        if not is_valid:
            # if valid, we have kept the exact same analysis, so None should remain
            assert latest_analysis[key] is not None
    # in all cases, profile should have been recreated
    assert latest_analysis["profile"] is not None


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


@pytest.mark.parametrize(
    "params",
    (
        ("col", False),
        ("çà€", True),
    ),
)
async def test_too_long_column_name(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
    params,
):
    col, has_non_ascii = params
    url = "http://example.com/csv"
    max_len = 10
    col_name = (col * ((max_len // len(col)) + 1))[: max_len if not has_non_ascii else max_len - 3]
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=f"{col_name},b,c\n1,2,3".encode("utf-8"),
        repeat=True,
    )
    # should fail because one column name is too long
    with patch("udata_hydra.config.NAMEDATALEN", max_len):
        await analyse_csv(check=check)
    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    # analysis failed
    assert updated_check["parsing_error"].startswith("scan_column_names:")
    # table was not created
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert table_name not in [r["table_name"] for r in tables]


@pytest.mark.parametrize(
    "db_to_parquet_enabled, expected_await_count",
    (
        (True, 1),
        (False, 0),
    ),
)
async def test_analyse_csv_db_to_parquet_switch(
    setup_catalog,
    rmock,
    catalog_content,
    fake_check,
    produce_mock,
    db_to_parquet_enabled,
    expected_await_count,
):
    """export_db_to_parquet runs only when DB_TO_PARQUET is enabled (analyse_csv path)."""
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body=catalog_content)

    with (
        patch("udata_hydra.config.DB_TO_PARQUET", db_to_parquet_enabled),
        patch(
            "udata_hydra.analysis.csv.export_db_to_parquet",
            new_callable=AsyncMock,
        ) as mock_export,
    ):
        mock_export.return_value = ("https://example.test/fake.parquet", 42)
        await analyse_csv(check=check)

    assert mock_export.await_count == expected_await_count
    if db_to_parquet_enabled:
        mock_export.assert_awaited_once()
        assert mock_export.await_args is not None
        kwargs = mock_export.await_args.kwargs
        assert kwargs["table_name"] == table_name
        assert kwargs["resource_id"] == RESOURCE_ID
        assert kwargs["check_id"] == check["id"]
        assert "inspection" in kwargs


async def test_crash_after_db_insertion(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
):
    def _crash(*args, **kwargs):
        raise Exception("BOOM")

    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=("a,b,c\n" + "1,2,3\n" * 200).encode("utf-8"),
        repeat=True,
    )
    with (
        patch("udata_hydra.config.DB_TO_PARQUET", True),
        patch(
            "udata_hydra.analysis.csv.export_db_to_parquet",
            new=_crash,
        ),
    ):
        # pretend the analysis crashes during parquet conversion
        await analyse_csv(check=check)
    # we should still have the table and its reference in tables_index
    await db.execute(f'SELECT * FROM "{table_name}"')
    rows = list(
        await db.fetch("SELECT * FROM tables_index WHERE resource_id = $1", check["resource_id"])
    )
    assert len(rows) == 1
    assert rows[0]["parsing_table"] == table_name
    # yet we have the error where we should
    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    assert updated_check["parsing_error"] is not None
    assert updated_check["parquet_url"] is None


async def test_file_with_nan(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
):
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=("a,b,c\n1,1.0,inf\n2,nan,2.0\n3,3.0,3.0\n").encode("utf-8"),
        repeat=True,
    )
    await analyse_csv(check=check)
    # it should all be fine
    rows = await db.fetch(f'SELECT * FROM "{table_name}"')
    assert dict(rows[0])["c"] == float("inf")
    assert dict(rows[1])["b"] is None
    profile = json.loads(
        list(
            await db.fetch(
                "SELECT * FROM tables_index WHERE resource_id = $1", check["resource_id"]
            )
        )[0]["csv_detective"]
    )["profile"]
    for col in ["a", "b"]:
        # NaN doesn't prevent the operations
        assert all(profile[col][method] is not None for method in ["min", "max", "mean", "std"])
    # inf does for max, mean and std
    assert all(profile["c"][method] is None for method in ["max", "mean", "std"])
    assert profile["c"]["min"] is not None

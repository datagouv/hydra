import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import ClientSession
from asyncpg.exceptions import UndefinedTableError
from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.analysis.csv import analyse_csv, csv_to_db
from udata_hydra.analysis.geojson import csv_to_geojson_and_pmtiles
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils.minio import MinIOClient

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
    assert resource["status"] is None
    assert resource["status_since"] is None

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
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
    assert resource["status"] is None

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
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
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            output_df=True,
            num_rows=-1,
            save_results=False,
        )
        assert inspection["separator"] == separator
        await csv_to_db(df=df, inspection=inspection, table_name="test_table")
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
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            encoding="utf-8",
            output_df=True,
            cast_json=False,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(df=df, inspection=inspection, table_name="test_table", debug_insert=True)
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
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            sep=",",
            output_df=True,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(df=df, inspection=inspection, table_name="test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res[injection] == "test"


async def test_percentage_column(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int,% mon pourcent\n1,test".encode("utf-8"))
        fp.seek(0)
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            output_df=True,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(df=df, inspection=inspection, table_name="test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["% mon pourcent"] == "test"


async def test_reserved_column_name(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int,xmin\n1,test".encode("utf-8"))
        fp.seek(0)
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            output_df=True,
            num_rows=-1,
            save_results=False,
        )
        await csv_to_db(df=df, inspection=inspection, table_name="test_table")
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
    assert resource["status"] is None

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert (
        res["parsing_error"]
        == "csv_detective:Could not detect the file's encoding. Consider specifying it in the routine call."
    )
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
    "header": ["a", "b"],
    "rows": [["1", "13002526500013"], ["5", "38271817900023"]],
    "encoding": "ASCII",
    "columns": {
        "a": {"score": 1.0, "format": "int", "python_type": "int"},
        "b": {"score": 1.0, "format": "siret", "python_type": "string"},
    },
    "header_row_idx": 0,
    "categorical": None,
    "formats": {"int": ["a"], "siret": ["b"]},
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
                "header": ["a", "c"],
                "columns": {
                    "a": {"score": 1.0, "format": "int", "python_type": "int"},
                    "c": {"score": 1.0, "format": "siret", "python_type": "string"},
                },
                "formats": {"int": ["a"], "siret": ["c"]},
            },
            False,
        ),
        # format changed
        (
            default_kwargs,
            default_kwargs
            | {
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
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)",
        table_name,
        json.dumps(previous_analysis),
        check.get("resource_id"),
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
    "params",
    (
        # csv to geojson is disabled
        ({}, None, False),
        # no geographical data in the file
        ({"data": ["rouge", "vert", "bleu", "jaune", "blanc"]}, None, True),
        # a column contains geometry
        (
            {
                "polyg": [
                    json.dumps(
                        {"type": "Point", "coordinates": [10 * k * (-1) ** k, 20 * k * (-1) ** k]}
                    )
                    for k in range(1, 6)
                ]
            },
            {"polyg": "geojson"},
            True,
        ),
        # a column contains coordinates (format: lat,lon)
        (
            {"coords": [f"{10 * k * (-1) ** k},{20 * k * (-1) ** k}" for k in range(1, 6)]},
            {"coords": "latlon_wgs"},
            True,
        ),
        # a column contains coordinates (format: [lon, lat])
        (
            {"lonlat": [f"[{20 * k * (-1) ** k}, {10 * k * (-1) ** k}]" for k in range(1, 6)]},
            {"lonlat": "lonlat_wgs"},
            True,
        ),
        # the table has latitude and longitude in separate columns
        (
            {
                "lat": [10 * k * (-1) ** k for k in range(1, 6)],
                "long": [20 * k * (-1) ** k for k in range(1, 6)],
            },
            {"lat": "latitude_wgs", "long": "longitude_wgs"},
            True,
        ),
    ),
)
async def test_csv_to_geojson_pmtiles(db, params, clean_db, mocker):
    other_columns = {
        "nombre": range(1, 6),
        "score": [0.01, 1.2, 34.5, 678.9, 10],
        "est_colonne": ["oui", "non", "non", "oui", "non"],
        "naissance": ["1996-02-13", "1995-02-06", "2000-01-28", "1998-02-20", "2015-04-23"],
    }
    geo_columns, expected_formats, patched_config = params
    sep = ";"
    columns = other_columns | geo_columns
    file = sep.join(columns) + "\n"
    for _ in range(5):
        file += sep.join(str(val) for val in [data[_] for data in columns.values()]) + "\n"

    with NamedTemporaryFile() as fp:
        fp.write(file.encode("utf-8"))
        fp.seek(0)
        inspection, df = csv_detective_routine(
            file_path=fp.name,
            output_profile=True,
            output_df=True,
            cast_json=False,
            num_rows=-1,
            save_results=False,
        )

    if expected_formats:
        for col in expected_formats:
            assert expected_formats[col] in inspection["columns"][col]["format"]

    with patch("udata_hydra.config.CSV_TO_GEOJSON", patched_config):
        if not patched_config or expected_formats is None:
            # process is disabled or early exit because no geo data
            with patch("udata_hydra.analysis.geojson.geojson_to_pmtiles") as mock_func:
                res = await csv_to_geojson_and_pmtiles(df, inspection, RESOURCE_ID)
                assert res is None
                mock_func.assert_not_called()
        else:
            minio_url = "my.minio.fr"
            geojson_bucket = "geojson_bucket"
            geojson_folder = "geojson_folder"
            pmtiles_bucket = "pmtiles_bucket"
            pmtiles_folder = "pmtiles_folder"
            mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
            mocked_minio = MagicMock()
            mocked_minio.fput_object.return_value = None
            mocked_minio.bucket_exists.return_value = True
            with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
                mocked_minio_client_geojson = MinIOClient(
                    bucket=geojson_bucket, folder=geojson_folder
                )
                mocked_minio_client_pmtiles = MinIOClient(
                    bucket=pmtiles_bucket, folder=pmtiles_folder
                )
            with (
                patch(
                    "udata_hydra.analysis.geojson.minio_client_geojson",
                    new=mocked_minio_client_geojson,
                ),
                patch(
                    "udata_hydra.analysis.geojson.minio_client_pmtiles",
                    new=mocked_minio_client_pmtiles,
                ),
            ):
                result = await csv_to_geojson_and_pmtiles(
                    df, inspection, RESOURCE_ID, cleanup=False
                )
                assert result is not None, (
                    "Expected geographical data to be processed, but function returned None"
                )
                (
                    geojson_filepath,
                    geojson_size,
                    geojson_url,
                    pmtiles_filepath,
                    pmtiles_size,
                    pmtiles_url,
                ) = result
            # checking geojson
            with open(f"{RESOURCE_ID}.geojson", "r") as f:
                geojson = json.load(f)
            assert all(key in geojson for key in ("type", "features"))
            assert len(geojson["features"]) == 5
            for feat in geojson["features"]:
                assert feat["type"] == "Feature"
                assert isinstance(feat["geometry"], dict)
                assert all(col in feat["properties"] for col in other_columns)
            assert (
                geojson_url
                == f"https://{minio_url}/{geojson_bucket}/{geojson_folder}/{RESOURCE_ID}.geojson"
            )
            assert isinstance(geojson_size, int)

            # checking PMTiles
            with open(f"{RESOURCE_ID}.pmtiles", "rb") as f:
                header = f.read(7)
            assert header == b"PMTiles"
            assert (
                pmtiles_url
                == f"https://{minio_url}/{pmtiles_bucket}/{pmtiles_folder}/{RESOURCE_ID}.pmtiles"
            )
            assert isinstance(pmtiles_size, int)

            # Clean up files after tests
            geojson_filepath.unlink()
            pmtiles_filepath.unlink()


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
    # analysis failed
    assert updated_check["parsing_error"].startswith("scan_column_names:")
    # table was not created
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert table_name not in [r["table_name"] for r in tables]

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest
from aiohttp import ClientSession
from asyncpg.exceptions import UndefinedTableError
from csv_detective import routine as csv_detective_routine, validate_then_detect
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.analysis.csv import analyse_csv, csv_to_db
from udata_hydra.crawl.check_resources import check_resource
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
    check = await fake_check()
    filename, expected_count = ("20190618-annuaire-diagnostiqueurs.csv", 45522)
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
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
    assert count["count"] == expected_count
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count


@pytest.mark.parametrize(
    "line_expected",
    (
        # (int, float, string, bool), (__id, int, float, string, bool)
        ("1,1020.20,test,true", (1, 1, 1020.2, "test", True), ","),
        ('2,"1020,20",test,false', (1, 2, 1020.2, "test", False), ","),
        ("1;1020.20;test;true", (1, 1, 1020.2, "test", True), ";"),
        ("2;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
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
        await csv_to_db(df=df, inspection=inspection, table_name="test_table")
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "int", "float", "string", "bool"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


@pytest.mark.parametrize(
    "line_expected",
    (
        # (json, date, datetime), (__id, json, date, datetime)
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
        fp.write(f"json;date;datetime\n{line}".encode("utf-8"))
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

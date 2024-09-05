import hashlib
import json
from datetime import date, datetime
from tempfile import NamedTemporaryFile

import pytest
from asyncpg.exceptions import UndefinedTableError
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra import config
from udata_hydra.analysis.csv import analyse_csv, csv_to_db
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

    # Analyse the CSV
    await analyse_csv(check_id=check["id"])

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource["status"] is None

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
    await analyse_csv(check_id=check["id"])

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


async def test_exception_analysis(setup_catalog, rmock, db, fake_check, produce_mock):
    """
    Tests that exception resources (files that are too large to be normally processed) are indeed processed.
    """
    save_config = config.MAX_FILESIZE_ALLOWED
    config.override(MAX_FILESIZE_ALLOWED={"csv": 5000})
    await db.execute(
        f"UPDATE catalog SET resource_id = '{config.LARGE_RESOURCES_EXCEPTIONS[0]}' WHERE id=1"
    )
    check = await fake_check(resource_id=config.LARGE_RESOURCES_EXCEPTIONS[0])
    filename, expected_count = ("20190618-annuaire-diagnostiqueurs.csv", 45522)
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    # Check resource status before analysis
    resource = await Resource.get(config.LARGE_RESOURCES_EXCEPTIONS[0])
    assert resource["status"] is None

    # Analyse the CSV
    await analyse_csv(check_id=check["id"])

    # Check resource status after analysis
    resource = await Resource.get(config.LARGE_RESOURCES_EXCEPTIONS[0])
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
    config.override(MAX_FILESIZE_ALLOWED=save_config)


@pytest.mark.parametrize(
    "line_expected",
    (
        # (int, float, string, bool), (__id, int, float, string, bool)
        ("1,1 020.20,test,true", (1, 1, 1020.2, "test", True), ","),
        ('2,"1 020,20",test,false', (1, 2, 1020.2, "test", False), ","),
        ("1;1 020.20;test;true", (1, 1, 1020.2, "test", True), ";"),
        ("2;1 020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0;1 020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
    ),
)
async def test_csv_to_db_simple_type_casting(db, line_expected, clean_db):
    line, expected, separator = line_expected
    with NamedTemporaryFile() as fp:
        fp.write(f"int, float, string, bool\n\r{line}".encode("utf-8"))
        fp.seek(0)
        columns = {
            "int": {"python_type": "int"},
            "float": {"python_type": "float"},
            "string": {"python_type": "string"},
            "bool": {"python_type": "bool"},
        }
        inspection = {
            "separator": separator,
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "header": list(columns.keys()),
            "columns": columns,
        }
        await csv_to_db(fp.name, inspection, "test_table")
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "int", "float", "string", "bool"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


@pytest.mark.parametrize(
    "line_expected",
    (
        # (json, date, datetime), (__id, json, date, datetime)
        (
            '{"a": 1};31 décembre 2022;2022-31-12 12:00:00',
            (1, json.dumps({"a": 1}), date(2022, 12, 31), datetime(2022, 12, 31, 12, 0, 0)),
        ),
        (
            '[{"a": 1, "b": 2}];31st december 2022;12-31-2022 12:00:00',
            (
                1,
                json.dumps([{"a": 1, "b": 2}]),
                date(2022, 12, 31),
                datetime(2022, 12, 31, 12, 0, 0),
            ),
        ),
    ),
)
async def test_csv_to_db_complex_type_casting(db, line_expected, clean_db):
    line, expected = line_expected
    with NamedTemporaryFile() as fp:
        fp.write(f"json, date, datetime\n\r{line}".encode("utf-8"))
        fp.seek(0)
        columns = {
            "json": {"python_type": "json"},
            "date": {"python_type": "date"},
            "datetime": {"python_type": "datetime"},
        }
        inspection = {
            "separator": ";",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "header": list(columns.keys()),
            "columns": columns,
        }
        # Insert the data
        await csv_to_db(fp.name, inspection, "test_table")
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "json", "date", "datetime"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


async def test_basic_sql_injection(db, clean_db):
    # tries to execute
    # CREATE TABLE table_name("int" integer, "col_name" text);DROP TABLE toto;--)
    injection = 'col_name" text);DROP TABLE toto;--'
    with NamedTemporaryFile() as fp:
        fp.write(f"int, {injection}\n\r1,test".encode("utf-8"))
        fp.seek(0)
        columns = {
            "int": {"python_type": "int"},
            injection: {"python_type": "string"},
        }
        inspection = {
            "separator": ",",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "header": list(columns.keys()),
            "columns": columns,
        }
        # Insert the data
        await csv_to_db(fp.name, inspection, "test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res[injection] == "test"


async def test_percentage_column(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int, % mon pourcent\n\r1,test".encode("utf-8"))
        fp.seek(0)
        columns = {
            "int": {"python_type": "int"},
            "% mon pourcent": {"python_type": "string"},
        }
        inspection = {
            "separator": ",",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "header": list(columns.keys()),
            "columns": columns,
        }
        # Insert the data
        await csv_to_db(fp.name, inspection, "test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["% mon pourcent"] == "test"


async def test_reserved_column_name(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int, xmin\n\r1,test".encode("utf-8"))
        fp.seek(0)
        columns = {
            "int": {"python_type": "int"},
            "xmin": {"python_type": "string"},
        }
        inspection = {
            "separator": ",",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "header": list(columns.keys()),
            "columns": columns,
        }
        # Insert the data
        await csv_to_db(fp.name, inspection, "test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["xmin__hydra_renamed"] == "test"


async def test_error_reporting_csv_detective(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body="".encode("utf-8"))

    # Analyse the CSV
    await analyse_csv(check_id=check["id"])

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource["status"] is None

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert res["parsing_error"] == "csv_detective:list index out of range"
    assert res["parsing_finished_at"]


async def test_error_reporting_parsing(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check()
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body="a,b,c\n1,2".encode("utf-8"))

    # Analyse the CSV
    await analyse_csv(check_id=check["id"])

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource["status"] is None

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert (
        res["parsing_error"]
        == "csv_detective:Number of columns is not even across the first 10 rows."
    )
    assert res["parsing_finished_at"]
    with pytest.raises(UndefinedTableError):
        await db.execute(f'SELECT * FROM "{table_name}"')


async def test_analyse_csv_url_param(rmock, catalog_content, clean_db):
    url = "https://example.com/another-url"
    rmock.get(url, status=200, body=catalog_content)
    await analyse_csv(url=url)


async def test_analyse_csv_send_udata_webhook(
    setup_catalog, rmock, catalog_content, db, fake_check, udata_url
):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    rmock.put(udata_url, status=200)

    # Analyse the CSV
    await analyse_csv(check_id=check["id"])

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource["status"] is None

    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("analysis:parsing:started_at")
    assert webhook.get("analysis:parsing:finished_at")
    assert webhook.get("analysis:parsing:error") is None

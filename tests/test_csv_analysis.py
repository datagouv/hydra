import hashlib
import json
import pytest

from tempfile import NamedTemporaryFile

from udata_hydra.utils.csv import analyse_csv, csv_to_db

from .conftest import RESOURCE_ID

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("optimized", [True, False])
async def test_analyse_csv_on_catalog(rmock, catalog_content, db, optimized, clean_db):
    url = "http://example.com/my.csv"
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body=catalog_content)
    await analyse_csv(url=url, optimized=optimized)
    res = await db.fetchrow("SELECT * FROM csv_analysis")
    assert res["parsing_table"] == table_name
    assert res["parsing_error"] is None
    inspection = json.loads(res["csv_detective"])
    assert all(k in inspection["columns"] for k in ["id", "url"])
    rows = list(await db.fetch(f'SELECT * FROM "{table_name}"'))
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == RESOURCE_ID
    assert row["url"] == "https://example.com/resource-1"


@pytest.mark.parametrize("params", [
    # pretty big one, with empty lines
    ("20190618-annuaire-diagnostiqueurs.csv", 45522),
])
async def test_analyse_csv_real_files(rmock, db, params, clean_db):
    filename, expected_count = params
    url = "http://example.com/my.csv"
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)
    await analyse_csv(url=url)
    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == expected_count


@pytest.mark.parametrize("line_expected", (
    ("1,1 020.20,test,true", (1, 1, 1020.2, "test", True), ","),
    ('2,"1 020,20",test,false', (1, 2, 1020.2, "test", False), ","),
    ("1;1 020.20;test;true", (1, 1, 1020.2, "test", True), ";"),
    ("2;1 020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
))
async def test_csv_to_db_type_casting(db, line_expected, clean_db):
    line, expected, separator = line_expected
    with NamedTemporaryFile() as fp:
        fp.write(f"int, float, string, bool\n\r{line}".encode("utf-8"))
        fp.seek(0)
        inspection = {
            "separator": separator,
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "columns": {
                "int": {"python_type": "int"},
                "float": {"python_type": "float"},
                "string": {"python_type": "string"},
                "bool": {"python_type": "bool"},
            }
        }
        await csv_to_db(fp.name, inspection, "test_table")
    res = list(await db.fetch("SELECT * FROM test_table"))
    assert len(res) == 1
    cols = ["__id", "int", "float", "string", "bool"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


async def test_basic_sql_injection(db, clean_db):
    # tries to execute
    # CREATE TABLE table_name("int" integer, "col_name" text);DROP TABLE toto;--)
    injection = 'col_name" text);DROP TABLE toto;--'
    with NamedTemporaryFile() as fp:
        fp.write(f"int, {injection}\n\r1,test".encode("utf-8"))
        fp.seek(0)
        inspection = {
            "separator": ",",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "columns": {
                "int": {"python_type": "int"},
                injection: {"python_type": "string"},
            }
        }
        await csv_to_db(fp.name, inspection, "test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res[injection] == "test"


async def test_percentage_column(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write("int, % mon pourcent\n\r1,test".encode("utf-8"))
        fp.seek(0)
        inspection = {
            "separator": ",",
            "encoding": "utf-8",
            "header_row_idx": 0,
            "total_lines": 1,
            "columns": {
                "int": {"python_type": "int"},
                "% mon pourcent": {"python_type": "string"},
            }
        }
        await csv_to_db(fp.name, inspection, "test_table")
    res = await db.fetchrow("SELECT * FROM test_table")
    assert res["% mon pourcent"] == "test"

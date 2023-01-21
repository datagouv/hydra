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


# TODO: parametrize line
async def test_csv_to_db_type_casting(db, clean_db):
    with NamedTemporaryFile() as fp:
        fp.write(b"""int, float, string, bool
1,1 020.20,test,true
2,"1 020,20",test,false
""")
        fp.seek(0)
        inspection = {
            "separator": ",",
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
        assert len(res) == 2
        assert dict(res[0]) == {
            "__id": 1,
            "int": 1,
            "float": 1020.2,
            "string": "test",
            "bool": True,
        }
        assert dict(res[1]) == {
            "__id": 2,
            "int": 2,
            "float": 1020.2,
            "string": "test",
            "bool": False,
        }

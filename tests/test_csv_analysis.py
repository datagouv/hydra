import hashlib
import json
import pytest

from udata_hydra.utils.csv import analyse_csv

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

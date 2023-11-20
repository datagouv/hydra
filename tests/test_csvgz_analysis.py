import hashlib
import json
import pytest

from udata_hydra.analysis.csv import analyse_csv

pytestmark = pytest.mark.asyncio


async def test_csvgz_analysis(setup_catalog, rmock, db, fake_check, produce_mock):
    check = await fake_check()
    filename, expected_count = ("donnee-dep-data.gouv-2022-geographie2023-produit-le2023-07-17.csv.gz", 9898)
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)
    await analyse_csv(check_id=check["id"])
    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == expected_count
    profile = await db.fetchrow("SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"])
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count

import hashlib

import pytest

from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.db.codec import parse_json_value

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "file_and_count",
    (
        ("20190618-annuaire-diagnostiqueurs_compressed.csv.gz", 29),
        ("catalog.xls", 2),
        ("catalog.xlsx", 2),
    ),
)
async def test_formats_analysis(setup_catalog, rmock, db, fake_check, produce_mock, file_and_count):
    check: dict = await fake_check()
    filename, expected_count = file_and_count
    url: str = check["url"]
    table_name: str = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)
    await analyse_csv(check=check)
    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == expected_count
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile: dict = parse_json_value(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count

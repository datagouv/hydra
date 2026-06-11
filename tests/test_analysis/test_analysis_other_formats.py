import hashlib
import json

import pytest

from udata_hydra.data_formats import Csvgz, Xls, Xlsx
from udata_hydra.data_formats.csv_like.analyse import analyse_csv

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "file_and_count",
    (
        (Csvgz, "20190618-annuaire-diagnostiqueurs_compressed.csv.gz", 29),
        (Xls, "catalog.xls", 2),
        (Xlsx, "catalog.xlsx", 2),
    ),
)
async def test_formats_analysis(setup_catalog, rmock, db, fake_check, produce_mock, file_and_count):
    data_format, filename, expected_count = file_and_count
    check: dict = await fake_check(headers={"content-type": data_format.standard_mime_type})
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
    profile: dict = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count

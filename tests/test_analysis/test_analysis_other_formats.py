import gzip
import hashlib
import json

import pytest
from asyncpg.exceptions import UndefinedTableError

from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.data_formats import Gz, Xls, Xlsx
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio

JSON_GZ_BODY = gzip.compress(b'{"structures": [{"id": 1}, {"id": 2}]}')


@pytest.mark.parametrize(
    "file_and_count",
    (
        (Gz, "20190618-annuaire-diagnostiqueurs_compressed.csv.gz", 29),
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
    file = await download_from_check(check, data_format)
    await file.analyse(check=check)
    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == expected_count
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile: dict = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count


async def test_gz_skips_json_payload(setup_catalog, rmock, db, fake_check, produce_mock):
    """A json.gz must not be ingested as CSV, even when served as application/gzip."""
    check: dict = await fake_check(
        headers={"content-type": "application/gzip"},
        url="https://example.com/finess.json.gz",
    )
    rmock.get(check["url"], status=200, body=JSON_GZ_BODY)
    file = await download_from_check(check, Gz)
    await file.analyse(check=check)

    res = await db.fetchrow("SELECT * FROM checks WHERE id = $1", check["id"])
    assert res["parsing_error"] is None
    assert res["parsing_table"] is None

    table_name: str = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    with pytest.raises(UndefinedTableError):
        await db.fetch(f'SELECT * FROM "{table_name}"')

    resource = await Resource.get(check["resource_id"])
    assert resource is not None
    assert resource["status"] is None


async def test_analyse_resource_skips_json_gz(setup_catalog, rmock, db, fake_check, produce_mock):
    check: dict = await fake_check(
        headers={"content-type": "application/gzip"},
        url="https://example.com/finess.json.gz",
    )
    rmock.get(check["url"], status=200, body=JSON_GZ_BODY)
    await analyse_resource(check=check, last_check=None)

    res = await db.fetchrow("SELECT * FROM checks WHERE id = $1", check["id"])
    assert res["parsing_error"] is None
    assert res["parsing_table"] is None
    assert res["mime_type"] == "application/json"  # inner payload, not application/gzip

    table_name: str = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    with pytest.raises(UndefinedTableError):
        await db.fetch(f'SELECT * FROM "{table_name}"')

    resource = await Resource.get(check["resource_id"])
    assert resource is not None
    assert resource["status"] is None

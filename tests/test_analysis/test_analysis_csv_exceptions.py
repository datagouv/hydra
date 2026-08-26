import hashlib
import json
import logging

import pytest
from asyncpg import Record

from tests.conftest import DATASET_ID, RESOURCE_EXCEPTION_ID, RESOURCE_EXCEPTION_TABLE_INDEXES
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.data_formats import Csv
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils.db import get_columns_with_indexes

pytestmark = pytest.mark.asyncio


log = logging.getLogger("udata-hydra")


async def test_exception_analysis(
    setup_catalog_with_resource_exception, rmock, db, fake_check, produce_mock, mocker
):
    """
    Tests that exception resources (files that are too large to be normally processed) are indeed processed.
    """
    # Change config to accept large files
    mocker.patch("udata_hydra.config.MAX_FILESIZE_ALLOWED", 5000)

    # Create a previous fake check for the resource
    check = await fake_check(resource_id=RESOURCE_EXCEPTION_ID)
    filename, expected_count = ("20190618-annuaire-diagnostiqueurs.csv", 45522)
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_EXCEPTION_ID)
    assert resource is not None
    assert resource["status"] is None

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_EXCEPTION_ID)
    assert resource is not None
    assert resource["status"] is None

    # Check the table has been created in CSV DB, with the expected number of rows, and get the columns
    row: Record = await db.fetchrow(f'SELECT *, count(*) over () AS count FROM "{table_name}"')
    assert row["count"] == expected_count

    # Check if indexes have been created for the table
    expected_columns_with_indexes = list(RESOURCE_EXCEPTION_TABLE_INDEXES.keys())
    expected_columns_with_indexes.append("__id")
    indexes: list[Record] | None = await get_columns_with_indexes(table_name)
    assert indexes
    for idx in indexes:
        assert idx["table_name"] == table_name
        assert idx["column_name"] in expected_columns_with_indexes

    # Check the profile has been saved in the tables_index
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count
    # every column name of that file fits in Postgres, so nothing was renamed
    assert profile["columns_mapping"] == {}


async def test_index_on_a_column_the_file_does_not_have(
    setup_catalog, rmock, db, fake_check, produce_mock
):
    """table_indexes is filled by hand and the file it points at can change: an index asked
    on a column that is not there must land in parsing_error, not escape the analysis job."""
    resource_id = "9f9ca6f7-5ee0-4d0f-a0da-3ba51ae4b9be"
    await Resource.insert(
        dataset_id=DATASET_ID,
        resource_id=resource_id,
        url="http://example.com/missing-index-column",
        type="main",
        format="csv",
        title="Resource indexed on a column it doesn't have",
    )
    await ResourceException.insert(
        resource_id=resource_id,
        table_indexes={"colonne_absente": "index"},
        comment="This is a test comment.",
    )
    check = await fake_check(resource_id=resource_id, headers={"content-type": "application/csv"})
    rmock.get(
        check["url"],
        status=200,
        headers={"content-type": "application/csv"},
        body=b"a,b\n1,2",
    )

    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    assert updated_check["parsing_error"].startswith("create_table_query:")
    assert "colonne_absente" in updated_check["parsing_error"]
    # the half-created table has been cleaned up
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert hashlib.md5(check["url"].encode("utf-8")).hexdigest() not in [
        r["table_name"] for r in tables
    ]

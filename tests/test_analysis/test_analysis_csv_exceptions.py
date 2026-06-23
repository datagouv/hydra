import hashlib
import json
import logging

import asyncpg
import pytest
from asyncpg import Record

from tests.conftest import RESOURCE_EXCEPTION_ID, RESOURCE_EXCEPTION_TABLE_INDEXES
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.data_formats import Csv
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


log = logging.getLogger("udata-hydra")


async def _get_columns_with_indexes(
    connection: asyncpg.Connection, table_name: str
) -> list[Record]:
    return await connection.fetch(
        """
            SELECT
                table_class.relname AS table_name,
                index_class.relname AS index_name,
                attribute.attname AS column_name
            FROM
                pg_class table_class,
                pg_class index_class,
                pg_index index_relation,
                pg_attribute attribute
            WHERE
                table_class.oid = index_relation.indrelid
                AND index_class.oid = index_relation.indexrelid
                AND attribute.attrelid = table_class.oid
                AND attribute.attnum = ANY(index_relation.indkey)
                AND table_class.relkind = 'r'
                AND table_class.relname = $1
            ORDER BY
                table_class.relname,
                index_class.relname;
        """,
        table_name,
    )


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
    indexes: list[Record] = await _get_columns_with_indexes(db, table_name)
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

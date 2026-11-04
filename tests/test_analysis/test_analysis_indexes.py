import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from tests.conftest import RESOURCE_ID, RESOURCE_TABLE_INDEXES
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.data_formats import Csv, Parquet
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.db.resource_index import ResourceIndex
from udata_hydra.utils.db import get_columns_with_indexes

pytestmark = pytest.mark.asyncio

MIGRATION = Path("udata_hydra/migrations/csv/20261104_add_resources_indexes.sql")


async def test_csv_indexes_created_without_exception(
    setup_catalog, rmock, catalog_content, fake_check, produce_mock
):
    """Indexes are created for any resource declaring them, not only for exceptions"""
    assert await ResourceException.get_by_resource_id(RESOURCE_ID) is None
    await ResourceIndex.replace(RESOURCE_ID, {"url": "index"})

    check = await fake_check(headers={"content-type": "text/csv"})
    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    rmock.get(check["url"], status=200, body=catalog_content)
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    indexes = await get_columns_with_indexes(table_name)
    assert {idx["column_name"] for idx in indexes} == {"__id", "url"}


async def test_parquet_indexes_created_without_exception(
    setup_catalog, rmock, fake_check, produce_mock
):
    assert await ResourceException.get_by_resource_id(RESOURCE_ID) is None
    await ResourceIndex.replace(RESOURCE_ID, {"siren": "index"})

    check = await fake_check(headers={"content-type": "application/vnd.apache.parquet"})
    df = pd.DataFrame({"siren": ["130025265", "110000122"], "nom": ["DINUM", "CNIL"]})
    rmock.get(check["url"], status=200, body=df.to_parquet())
    file = await download_from_check(check, Parquet)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        table = await file.analyse(check=check)

    indexes = await get_columns_with_indexes(table.table_name)
    assert {idx["column_name"] for idx in indexes} == {"__id", "siren"}


async def test_index_on_a_column_missing_from_the_file_is_skipped(
    setup_catalog, rmock, catalog_content, db, fake_check, produce_mock
):
    """A declaration outlives the file it was written for: a column renamed or removed upstream
    must not prevent the conversion, nor leave the resource without a table"""
    await ResourceIndex.replace(RESOURCE_ID, {"url": "index", "removed_column": "index"})

    check = await fake_check(headers={"content-type": "text/csv"})
    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    rmock.get(check["url"], status=200, body=catalog_content)
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    indexes = await get_columns_with_indexes(table_name)
    assert {idx["column_name"] for idx in indexes} == {"__id", "url"}
    assert len(await db.fetch(f'SELECT * FROM "{table_name}"')) == 2


async def test_migration_copies_indexes_from_exceptions(setup_catalog, db):
    """Indexes declared on exceptions before resources_indexes existed are carried over"""
    # Legacy state: indexes stored on the exception, as written by the former exceptions endpoints.
    await db.execute(
        "INSERT INTO resources_exceptions (resource_id, table_indexes) VALUES ($1, $2)",
        RESOURCE_ID,
        json.dumps(RESOURCE_TABLE_INDEXES),
    )

    await db.execute(MIGRATION.read_text())

    assert await ResourceIndex.get_by_resource_id(RESOURCE_ID) == RESOURCE_TABLE_INDEXES

import hashlib

import nest_asyncio2 as nest_asyncio
import pytest

from udata_hydra import context
from udata_hydra.cli import analyse_csv_cli
from udata_hydra.db.check import Check

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_csv_analysis_from_check_id(
    setup_catalog, rmock, catalog_content, db, fake_check, produce_mock
):
    """Test the analyse-csv CLI command using check_id"""
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    await analyse_csv_cli(check_id=str(check["id"]))


async def test_csv_analysis_with_debug_insert(setup_catalog, rmock, db, fake_check, produce_mock):
    """Test the analyse-csv CLI command with debug insert mode"""

    # Create a check for an existing URL
    check = await fake_check()
    url = check["url"]

    # Use simple CSV content instead of catalog_content
    csv_content = "name,value\nAlice,42\nBob,73\nCharlie,91"
    rmock.get(url, status=200, body=csv_content)

    # Test with debug insert enabled
    await analyse_csv_cli(check_id=None, url=url, debug_insert=True)

    # Verify that the CSV table was created and contains data
    csv_pool = await context.pool("csv")

    # Find the table that was created (it uses MD5 hash of URL)
    table_hash = hashlib.md5(url.encode()).hexdigest()

    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert table_entry is not None, f"Table {table_hash} should exist in tables_index"

    # Verify table content (debug insert should work the same as normal insert)
    table_data = await csv_pool.fetch(f'SELECT * FROM "{table_hash}" ORDER BY name')
    assert len(table_data) == 3  # Should have 3 rows
    assert table_data[0]["name"] == "Alice"
    assert table_data[0]["value"] == 42


async def test_csv_analysis_from_url(
    setup_catalog, rmock, catalog_content, db, fake_check, produce_mock
):
    """Test the analyse-csv CLI command using URL, with an existing check"""
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    await analyse_csv_cli(check_id=None, url=url, debug_insert=False)


async def test_csv_analysis_from_external_url(setup_catalog, rmock, db, produce_mock):
    """Test the analyse-csv CLI command using an external URL with no existing check"""

    # Create a simple CSV content for testing
    csv_content = "name,age,city\nJohn,30,Paris\nJane,25,London\nBob,35,Berlin"

    # Mock an external URL
    external_url = "https://external-example.com/test.csv"
    rmock.get(external_url, status=200, body=csv_content)

    await analyse_csv_cli(check_id=None, url=external_url, resource_id=None, debug_insert=False)

    # Verify that no permanent data was created in the main database
    resources = await db.fetch("SELECT * FROM catalog WHERE url = $1", external_url)
    assert len(resources) == 0

    checks = await Check.get_by_url(external_url)
    assert len(checks) == 0

    # Verify that temporary CSV table was created and then cleaned up
    csv_pool = await context.pool("csv")

    # Check that no tables remain for this URL (they should be cleaned up)

    table_hash = hashlib.md5(external_url.encode()).hexdigest()

    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert table_entry is None, f"Table {table_hash} should not exist in tables_index after cleanup"


async def test_csv_analysis_from_external_invalid_url(setup_catalog, rmock, db, produce_mock):
    """Test the analyse-csv CLI command with invalid URL"""

    # Mock an invalid URL that returns an error
    invalid_url = "https://invalid-example.com/error.csv"
    rmock.get(invalid_url, status=404, body="Not Found")

    # Test should handle the error gracefully
    await analyse_csv_cli(check_id=None, url=invalid_url, resource_id=None, debug_insert=False)

    # Verify that no permanent data was created
    resources = await db.fetch("SELECT * FROM catalog WHERE url = $1", invalid_url)
    assert len(resources) == 0

    checks = await Check.get_by_url(invalid_url)
    assert len(checks) == 0

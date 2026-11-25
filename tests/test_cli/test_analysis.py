import json

import nest_asyncio2 as nest_asyncio
import pytest

from udata_hydra import context
from udata_hydra.cli import analyse_csv_cli
from udata_hydra.db import parsing_table_name
from udata_hydra.db.check import Check

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


@pytest.mark.parametrize(
    "entry_mode",
    [
        pytest.param("check_id", id="check_id"),
        pytest.param("url", id="url"),
    ],
)
async def test_csv_analysis_from_catalog(
    setup_catalog, rmock, catalog_content, fake_check, produce_mock, entry_mode
):
    """Test the analyse-csv CLI command using check_id or URL for a catalog resource."""
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    if entry_mode == "check_id":
        await analyse_csv_cli(check_id=str(check["id"]))
    else:
        await analyse_csv_cli(check_id=None, url=url, debug_insert=False)


async def test_csv_analysis_ignores_the_previous_analysis(
    setup_catalog, rmock, db, fake_check, produce_mock
):
    """A manual analysis is asked when the previous one looks wrong: it must not be kept"""
    check = await fake_check(headers={"content-type": "text/csv"})
    table_name = parsing_table_name(check["url"])
    # a previous analysis too loose for the data, which the file still fits: integers as strings
    previous_analysis = {
        "encoding": "utf-8",
        "separator": ",",
        "header_row_idx": 0,
        "header": ["a"],
        "columns": {"a": {"score": 1.0, "format": "string", "python_type": "string"}},
        "categorical": None,
        "formats": {"string": ["a"]},
        "columns_fields": None,
        "columns_labels": None,
        "profile": None,
        "total_lines": 2,
    }
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) "
        "VALUES($1, $2, $3, $4, $5)",
        table_name,
        json.dumps(previous_analysis),
        check["resource_id"],
        check["dataset_id"],
        check["url"],
    )
    await db.execute(f'CREATE TABLE "{table_name}"(__id serial PRIMARY KEY, a VARCHAR)')
    await db.execute(f"INSERT INTO \"{table_name}\" (a) VALUES ('1'), ('5')")
    rmock.get(check["url"], status=200, body="a\n1\n5\n")

    await analyse_csv_cli(check_id=str(check["id"]))

    res = await db.fetch(
        "SELECT csv_detective FROM tables_index WHERE resource_id=$1 ORDER BY created_at DESC",
        check["resource_id"],
    )
    assert len(res) == 2
    assert json.loads(res[0]["csv_detective"])["columns"]["a"]["format"] == "int"


async def test_csv_analysis_with_debug_insert(setup_catalog, rmock, db, fake_check, produce_mock):
    """Test the analyse-csv CLI command with debug insert mode"""

    # Create a check for an existing URL
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]

    # Use simple CSV content instead of catalog_content
    csv_content = "name,value\nAlice,42\nBob,73\nCharlie,91"
    rmock.get(url, status=200, body=csv_content)

    # Test with debug insert enabled
    await analyse_csv_cli(check_id=None, url=url, debug_insert=True)

    # Verify that the CSV table was created and contains data
    csv_pool = await context.pool("csv")

    # Find the table that was created (it uses MD5 hash of URL)
    table_hash = parsing_table_name(url)

    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert table_entry is not None, f"Table {table_hash} should exist in tables_index"

    # Verify table content (debug insert should work the same as normal insert)
    table_data = await csv_pool.fetch(f'SELECT * FROM "{table_hash}" ORDER BY name')
    assert len(table_data) == 3  # Should have 3 rows
    assert table_data[0]["name"] == "Alice"
    assert table_data[0]["value"] == 42


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

    table_hash = parsing_table_name(external_url)

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

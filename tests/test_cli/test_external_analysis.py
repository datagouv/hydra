import nest_asyncio
import pytest

from udata_hydra import context
from udata_hydra.cli import analyse_external_csv_cli

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_analyse_external_csv(setup_catalog, rmock, db):
    """Test the analyse-external-csv CLI command for external URLs with cleanup enabled"""

    # Create a simple CSV content for testing
    csv_content = "name,age,city\nJohn,30,Paris\nJane,25,London\nBob,35,Berlin"

    # Mock an external URL
    external_url = "https://external-example.com/test.csv"
    rmock.get(external_url, status=200, body=csv_content)

    # Test with cleanup enabled (default)
    await analyse_external_csv_cli(external_url, debug_insert=False, cleanup=True)

    # Verify that no permanent data was created in the main database
    resources = await db.fetch("SELECT * FROM catalog WHERE url = $1", external_url)
    assert len(resources) == 0

    checks = await db.fetch("SELECT * FROM checks WHERE url = $1", external_url)
    assert len(checks) == 0

    # Verify that temporary CSV table was created and then cleaned up

    csv_pool = await context.pool("csv")

    # Check that no tables remain for this URL (they should be cleaned up)
    import hashlib

    table_hash = hashlib.md5(external_url.encode()).hexdigest()

    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert table_entry is None, f"Table {table_hash} should not exist in tables_index after cleanup"


async def test_analyse_external_csv_no_cleanup(setup_catalog, rmock, db):
    """Test the analyse-external-csv CLI command without cleanup for inspection"""
    from udata_hydra.cli import analyse_external_csv_cli

    # Create a simple CSV content for testing
    csv_content = "id,value\n1,100\n2,200\n3,300"

    # Mock an external URL
    external_url = "https://external-example.com/data.csv"
    rmock.get(external_url, status=200, body=csv_content)

    # Test with cleanup disabled
    await analyse_external_csv_cli(external_url, debug_insert=False, cleanup=False)

    # Verify that temporary CSV table was created and NOT cleaned up
    from udata_hydra import context

    csv_pool = await context.pool("csv")

    # Find the table that was created (it uses MD5 hash of URL, not 'temp_' prefix)
    import hashlib

    table_hash = hashlib.md5(external_url.encode()).hexdigest()

    # Check if the table exists in tables_index
    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert (
        table_entry is not None
    ), f"Table {table_hash} should exist in tables_index when cleanup is disabled"

    temp_table_name = table_hash

    # Table should exist after analysis (no cleanup)
    table_exists = await csv_pool.fetchrow(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        temp_table_name,
    )
    assert table_exists["exists"]

    # Verify table content
    table_data = await csv_pool.fetch(f'SELECT * FROM "{temp_table_name}" ORDER BY id')
    assert len(table_data) == 3
    assert table_data[0]["id"] == 1
    assert table_data[0]["value"] == 100

    # Clean up manually for this test
    await csv_pool.execute(f'DROP TABLE IF EXISTS "{temp_table_name}"')
    await csv_pool.execute(f"DELETE FROM tables_index WHERE parsing_table = '{temp_table_name}'")


async def test_analyse_external_csv_debug_insert(setup_catalog, rmock, db):
    """Test the analyse-external-csv CLI command with debug insert mode"""
    from udata_hydra.cli import analyse_external_csv_cli

    # Create a simple CSV content for testing
    csv_content = "name,value\nAlice,42\nBob,73\nCharlie,91"

    # Mock an external URL
    external_url = "https://external-example.com/debug.csv"
    rmock.get(external_url, status=200, body=csv_content)

    # Test with debug insert enabled and cleanup disabled
    await analyse_external_csv_cli(external_url, debug_insert=True, cleanup=False)

    # Verify that temporary CSV table was created
    from udata_hydra import context

    csv_pool = await context.pool("csv")

    # Find the table that was created (it uses MD5 hash of URL)
    import hashlib

    table_hash = hashlib.md5(external_url.encode()).hexdigest()

    table_entry = await csv_pool.fetchrow(
        "SELECT * FROM tables_index WHERE parsing_table = $1", table_hash
    )
    assert (
        table_entry is not None
    ), f"Table {table_hash} should exist in tables_index when cleanup is disabled"

    temp_table_name = table_hash

    # Table should exist after analysis
    table_exists = await csv_pool.fetchrow(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        temp_table_name,
    )
    assert table_exists["exists"]

    # Verify table content (debug insert should work the same as normal insert)
    table_data = await csv_pool.fetch(f'SELECT * FROM "{temp_table_name}" ORDER BY name')
    assert len(table_data) == 3
    assert table_data[0]["name"] == "Alice"
    assert table_data[0]["value"] == 42

    # Clean up manually for this test
    await csv_pool.execute(f'DROP TABLE IF EXISTS "{temp_table_name}"')
    await csv_pool.execute(f"DELETE FROM tables_index WHERE parsing_table = '{temp_table_name}'")


async def test_analyse_external_csv_invalid_url(setup_catalog, rmock, db):
    """Test the analyse-external-csv CLI command with an invalid URL"""
    from udata_hydra.cli import analyse_external_csv_cli

    # Mock an invalid URL that returns an error
    invalid_url = "https://invalid-example.com/error.csv"
    rmock.get(invalid_url, status=404, body="Not Found")

    # Test should handle the error gracefully
    await analyse_external_csv_cli(invalid_url, debug_insert=False, cleanup=True)

    # Verify that no permanent data was created
    resources = await db.fetch("SELECT * FROM catalog WHERE url = $1", invalid_url)
    assert len(resources) == 0

    checks = await db.fetch("SELECT * FROM checks WHERE url = $1", invalid_url)
    assert len(checks) == 0

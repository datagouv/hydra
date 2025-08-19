import nest_asyncio
import pytest
from minicli import run

from tests.conftest import RESOURCE_URL

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_analysis_csv(setup_catalog, rmock, catalog_content, db, fake_check, produce_mock):
    """Test the analyse-csv CLI command"""
    # Analyse using check_id
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    run("analyse-csv", check_id=str(check["id"]))

    # Analyse using URL
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    run("analyse-csv", url=RESOURCE_URL)

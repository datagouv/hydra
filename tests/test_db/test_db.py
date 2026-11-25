import pytest

from udata_hydra.db import parsing_table_name

pytestmark = pytest.mark.asyncio


async def test_parsing_table_name():
    assert parsing_table_name("https://example.com/data.csv") == "2a01ab0de708440185cbb6473893860c"


async def test_parsing_table_name_matches_the_purge_sql(db):
    """`_purge_csv_tables` finds the tables to drop with `md5(url)` in SQL."""
    url = "https://example.com/données.csv"
    assert parsing_table_name(url) == await db.fetchval("SELECT md5($1)", url)

import hashlib
import json

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.tables_index import get_previous_inspection

pytestmark = pytest.mark.asyncio


async def _insert_previous_analysis(db, inspection: dict, sql_columns: str) -> str:
    table_name = hashlib.md5(b"http://example.com/csv").hexdigest()
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id) VALUES($1, $2, $3)",
        table_name,
        json.dumps(inspection),
        RESOURCE_ID,
    )
    await db.execute(f'CREATE TABLE "{table_name}"(__id serial PRIMARY KEY, {sql_columns})')
    await db.execute(f'INSERT INTO "{table_name}" DEFAULT VALUES')
    return table_name


async def test_previous_inspection_column_order_comes_from_header(db, clean_db):
    """JSONB reorders the keys of `columns`, so the order has to come from `header`."""
    header = ["zebra", "alpha", "milieu"]
    inspection = {
        "header": header,
        "columns": {col: {"python_type": "string", "format": "string"} for col in header},
    }
    await _insert_previous_analysis(db, inspection, '"zebra" text, "alpha" text, "milieu" text')

    previous = await get_previous_inspection(RESOURCE_ID)
    assert previous is not None
    assert list(previous["columns"]) == header


async def test_previous_inspection_with_a_renamed_column(db, clean_db):
    """The table column is `xmin__hydra_renamed` while the inspection knows it as `xmin`."""
    inspection = {
        "header": ["xmin", "other"],
        "columns": {
            "xmin": {"python_type": "string", "format": "string"},
            "other": {"python_type": "string", "format": "string"},
        },
        "columns_mapping": {"xmin": "xmin__hydra_renamed"},
    }
    await _insert_previous_analysis(db, inspection, '"xmin__hydra_renamed" text, "other" text')

    previous = await get_previous_inspection(RESOURCE_ID)
    assert previous is not None
    assert list(previous["columns"]) == ["xmin", "other"]
    # the mapping is always recomputed by to_db, a stale one must not be carried over
    assert "columns_mapping" not in previous


async def test_previous_inspection_ignored_when_header_and_columns_disagree(db, clean_db):
    inspection = {
        "header": ["a", "b"],
        "columns": {"a": {"python_type": "string", "format": "string"}},
    }
    await _insert_previous_analysis(db, inspection, '"a" text, "b" text')

    assert await get_previous_inspection(RESOURCE_ID) is None

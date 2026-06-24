import csv
import json
from pathlib import Path

import nest_asyncio2 as nest_asyncio
import pytest

from tests.conftest import DATASET_ID, RESOURCE_ID, RESOURCE_URL
from udata_hydra.cli.db import _dump_tables_index

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()

CSV_HEADERS = [
    "id",
    "parsing_table",
    "csv_detective",
    "resource_id",
    "dataset_id",
    "url",
    "created_at",
    "indexes",
    "deleted_at",
]


async def _insert_tables_index_row(
    db,
    *,
    parsing_table: str,
    resource_id: str = RESOURCE_ID,
    csv_detective: dict | None = None,
    indexes: dict | None = None,
    deleted: bool = False,
) -> None:
    await db.execute(
        """
        INSERT INTO tables_index(
            parsing_table, csv_detective, resource_id, dataset_id, url, indexes
        ) VALUES($1, $2, $3, $4, $5, $6)
        """,
        parsing_table,
        json.dumps(csv_detective or {"columns": {"id": {"python_type": "int"}}}),
        resource_id,
        DATASET_ID,
        RESOURCE_URL,
        json.dumps(indexes) if indexes is not None else None,
    )
    if deleted:
        await db.execute(
            "UPDATE tables_index SET deleted_at = NOW() WHERE parsing_table = $1",
            parsing_table,
        )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as csvfile:
        return list(csv.DictReader(csvfile))


async def test_dump_tables_index_exports_rows(clean_db, db, tmp_path):
    profile = {"columns": {"name": {"python_type": "string"}}}
    indexes = {"name": "index"}
    await _insert_tables_index_row(
        db,
        parsing_table="abc123",
        csv_detective=profile,
        indexes=indexes,
    )

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output)

    rows = _read_csv(output)
    assert list(rows[0].keys()) == CSV_HEADERS
    assert len(rows) == 1
    assert rows[0]["parsing_table"] == "abc123"
    assert json.loads(rows[0]["csv_detective"]) == profile
    assert json.loads(rows[0]["indexes"]) == indexes
    assert rows[0]["resource_id"] == RESOURCE_ID
    assert rows[0]["deleted_at"] == ""


async def test_dump_tables_index_excludes_deleted_by_default(clean_db, db, tmp_path):
    await _insert_tables_index_row(db, parsing_table="active", deleted=False)
    await _insert_tables_index_row(
        db,
        parsing_table="deleted",
        resource_id="5d0b2b91-b21b-4120-83ef-83f818ba2451",
        deleted=True,
    )

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output)

    rows = _read_csv(output)
    assert len(rows) == 1
    assert rows[0]["parsing_table"] == "active"


async def test_dump_tables_index_include_deleted(clean_db, db, tmp_path):
    await _insert_tables_index_row(db, parsing_table="active", deleted=False)
    await _insert_tables_index_row(
        db,
        parsing_table="deleted",
        resource_id="5d0b2b91-b21b-4120-83ef-83f818ba2451",
        deleted=True,
    )

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output, include_deleted=True)

    rows = _read_csv(output)
    assert len(rows) == 2
    parsing_tables = {row["parsing_table"] for row in rows}
    assert parsing_tables == {"active", "deleted"}


async def test_dump_tables_index_filter_by_resource_id(clean_db, db, tmp_path):
    other_resource_id = "5d0b2b91-b21b-4120-83ef-83f818ba2451"
    await _insert_tables_index_row(db, parsing_table="match", resource_id=RESOURCE_ID)
    await _insert_tables_index_row(db, parsing_table="other", resource_id=other_resource_id)

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output, resource_id=RESOURCE_ID)

    rows = _read_csv(output)
    assert len(rows) == 1
    assert rows[0]["parsing_table"] == "match"


async def test_dump_tables_index_limit(clean_db, db, tmp_path):
    await _insert_tables_index_row(
        db, parsing_table="first", resource_id="11111111-1111-1111-1111-111111111111"
    )
    await _insert_tables_index_row(
        db, parsing_table="second", resource_id="22222222-2222-2222-2222-222222222222"
    )

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output, limit=1)

    rows = _read_csv(output)
    assert len(rows) == 1

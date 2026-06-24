import csv
import json
from pathlib import Path

import nest_asyncio2 as nest_asyncio
import pytest

from tests.conftest import DATASET_ID, NOT_EXISTING_RESOURCE_ID, RESOURCE_ID, RESOURCE_URL
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


@pytest.mark.parametrize(
    "rows,kwargs,expected_count,expected_tables",
    [
        pytest.param(
            [("active", RESOURCE_ID, False), ("deleted", NOT_EXISTING_RESOURCE_ID, True)],
            {},
            1,
            {"active"},
            id="exclude_deleted_by_default",
        ),
        pytest.param(
            [("active", RESOURCE_ID, False), ("deleted", NOT_EXISTING_RESOURCE_ID, True)],
            {"include_deleted": True},
            2,
            {"active", "deleted"},
            id="include_deleted",
        ),
        pytest.param(
            [("match", RESOURCE_ID, False), ("other", NOT_EXISTING_RESOURCE_ID, False)],
            {"resource_id": RESOURCE_ID},
            1,
            {"match"},
            id="filter_by_resource_id",
        ),
    ],
)
async def test_dump_tables_index_filters(
    clean_db, db, tmp_path, rows, kwargs, expected_count, expected_tables
):
    for parsing_table, resource_id, deleted in rows:
        await _insert_tables_index_row(
            db,
            parsing_table=parsing_table,
            resource_id=resource_id,
            deleted=deleted,
        )

    output = tmp_path / "tables_index.csv"
    await _dump_tables_index(output=output, **kwargs)

    result = _read_csv(output)
    assert len(result) == expected_count
    if expected_tables is not None:
        assert {row["parsing_table"] for row in result} == expected_tables

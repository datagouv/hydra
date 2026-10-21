import json
import uuid

import nest_asyncio2 as nest_asyncio
import pytest

from tests.conftest import DATASET_ID
from udata_hydra.analysis.exports import export_parquet
from udata_hydra.cli.analysis import backfill_parquet_cli
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def _parsed_resource(
    db,
    fake_check,
    *,
    url: str,
    format: str = "csv",
    status: str | None = None,
    deleted: bool = False,
    parsing_error: str | None = None,
    parquet_url: bool = False,
    with_table: bool = True,
    with_rows: bool = True,
    total_lines: int = 2,
) -> dict:
    resource_id = str(uuid.uuid4())
    await Resource.insert(
        dataset_id=DATASET_ID,
        resource_id=resource_id,
        url=url,
        type="main",
        format=format,
        title="Backfill resource",
        status=status,
    )
    check = await fake_check(
        resource_id=resource_id,
        url=url,
        parsing_table=True,
        parquet_url=parquet_url,
    )
    if parsing_error:
        await Check.update(check["id"], {"parsing_error": parsing_error})
    if deleted:
        await Resource.delete(resource_id)

    if with_table:
        parsing_table = check["parsing_table"]
        inspection = {
            "total_lines": total_lines,
            "columns": {
                "a": {"python_type": "string", "format": "string"},
                "b": {"python_type": "string", "format": "string"},
            },
        }
        await db.execute(
            "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) "
            "VALUES($1, $2, $3, $4, $5)",
            parsing_table,
            json.dumps(inspection),
            resource_id,
            DATASET_ID,
            url,
        )
        # Column order in the live table differs from the JSONB key order above.
        await db.execute(
            f'CREATE TABLE "{parsing_table}" (__id serial PRIMARY KEY, b text, a text)'
        )
        if with_rows:
            await db.execute(
                f'INSERT INTO "{parsing_table}" (b, a) VALUES ($1, $2)',
                "x",
                "1",
            )
    return check


async def test_backfill_parquet_enqueues_only_parsed_resources_without_parquet(
    clean_db, db, fake_check, mocker
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    eligible = await _parsed_resource(
        db, fake_check, url="https://example.com/eligible.csv", total_lines=4
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/already.parquet.csv",
        parquet_url=True,
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/failed.csv",
        parsing_error="parquet_export:error",
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/native.parquet",
        format="parquet",
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/busy.csv",
        status="INSERTING_IN_DB",
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/deleted.csv",
        deleted=True,
    )
    await _parsed_resource(
        db,
        fake_check,
        url="https://example.com/empty.csv",
        with_rows=False,
        total_lines=0,
    )

    result = await backfill_parquet_cli(dry_run=False, limit=None)

    assert result["enqueued"] == 1
    assert result["skipped"] == 1
    assert result["min_lines"] == 4
    assert result["max_lines"] == 4
    assert mock_enqueue.call_count == 1
    args, kwargs = mock_enqueue.call_args
    assert args[0] is export_parquet
    assert kwargs["_priority"] == "low"
    assert kwargs["check"]["id"] == eligible["id"]
    assert kwargs["check"]["parsing_table"] == eligible["parsing_table"]
    assert kwargs["check"]["resource_id"] == eligible["resource_id"]
    table = kwargs["table"]
    assert table.table_name == eligible["parsing_table"]
    assert table.dataset_id == DATASET_ID
    assert list(table.inspection["columns"]) == ["b", "a"]


async def test_backfill_parquet_dry_run_reports_line_counts_without_enqueue(
    clean_db, db, fake_check, mocker
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    await _parsed_resource(db, fake_check, url="https://example.com/small.csv", total_lines=3)
    await _parsed_resource(db, fake_check, url="https://example.com/larger.csv", total_lines=12)

    result = await backfill_parquet_cli(dry_run=True, limit=None)

    assert result["dry_run"] is True
    assert result["examined"] == 2
    assert result["skipped"] == 0
    assert result["enqueued"] == 2
    assert result["min_lines"] == 3
    assert result["max_lines"] == 12
    mock_enqueue.assert_not_called()


async def test_backfill_parquet_limit_stops_after_n_exports(clean_db, db, fake_check, mocker):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    first = await _parsed_resource(
        db, fake_check, url="https://example.com/first.csv", total_lines=1
    )
    await _parsed_resource(db, fake_check, url="https://example.com/second.csv", total_lines=9)

    result = await backfill_parquet_cli(dry_run=False, limit=1)

    assert result["enqueued"] == 1
    assert result["examined"] == 1
    assert mock_enqueue.call_count == 1
    assert mock_enqueue.call_args.kwargs["check"]["id"] == first["id"]
    assert result["min_lines"] == 1
    assert result["max_lines"] == 1

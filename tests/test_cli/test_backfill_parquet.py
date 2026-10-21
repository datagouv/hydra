import json
import uuid

import nest_asyncio2 as nest_asyncio
import pytest

from tests.conftest import DATASET_ID
from udata_hydra.analysis.exports import export_parquet
from udata_hydra.cli.analysis import backfill_parquet_cli
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def _seed_parsed_resource(db, fake_check, url: str) -> dict:
    resource_id = str(uuid.uuid4())
    await Resource.insert(
        dataset_id=DATASET_ID,
        resource_id=resource_id,
        url=url,
        type="main",
        format="csv",
        title="Backfill resource",
    )
    check = await fake_check(resource_id=resource_id, url=url, parsing_table=True)
    parsing_table = check["parsing_table"]
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) "
        "VALUES($1, $2, $3, $4, $5)",
        parsing_table,
        json.dumps(
            {
                "total_lines": 4,
                "columns": {
                    "a": {"python_type": "string"},
                    "b": {"python_type": "string"},
                },
            }
        ),
        resource_id,
        DATASET_ID,
        url,
    )
    # JSONB does not keep this key order; the table does.
    await db.execute(f'CREATE TABLE "{parsing_table}" (__id serial PRIMARY KEY, b text, a text)')
    await db.execute(f'INSERT INTO "{parsing_table}" (b, a) VALUES ($1, $2)', "x", "1")
    return check


async def test_backfill_parquet_enqueues_parsed_resource(clean_db, db, fake_check, mocker):
    """A parsed resource with no parquet file is queued from the live table column order."""
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    check = await _seed_parsed_resource(db, fake_check, "https://example.com/eligible.csv")

    assert (await backfill_parquet_cli(dry_run=True))["enqueued"] == 1
    mock_enqueue.assert_not_called()

    await backfill_parquet_cli(dry_run=False)

    mock_enqueue.assert_called_once()
    assert mock_enqueue.call_args.args[0] is export_parquet
    assert mock_enqueue.call_args.kwargs["_priority"] == "low"
    assert mock_enqueue.call_args.kwargs["check"]["parsing_table"] == check["parsing_table"]
    assert list(mock_enqueue.call_args.kwargs["table"].inspection["columns"]) == ["b", "a"]


async def test_backfill_parquet_limit_enqueues_the_first_candidate(
    clean_db, db, fake_check, mocker
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    first = await _seed_parsed_resource(db, fake_check, "https://example.com/first.csv")
    await _seed_parsed_resource(db, fake_check, "https://example.com/second.csv")

    assert (await backfill_parquet_cli(dry_run=False, limit=1))["enqueued"] == 1
    mock_enqueue.assert_called_once()
    assert mock_enqueue.call_args.kwargs["check"]["id"] == first["id"]

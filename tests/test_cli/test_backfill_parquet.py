import csv
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

BACKFILL_STATUS = "TO_BACKFILL_PARQUET"


async def _backfill(**kwargs) -> dict:
    """Call the command with every option, typer defaults are not usable outside the CLI"""
    options = {"catalog_url": None, "catalog_file": None, "dry_run": False, "limit": None}
    return await backfill_parquet_cli(**{**options, **kwargs})


def _write_catalog(path, rows: list[dict]) -> str:
    """Write a minimal resources catalog, as exported by data.gouv.fr"""
    with open(path, "w", newline="") as fd:
        writer = csv.DictWriter(
            fd,
            fieldnames=["dataset.id", "dataset.archived", "id", "format", "extras"],
            delimiter=";",
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "dataset.id": DATASET_ID,
                    "dataset.archived": row.get("archived", "False"),
                    "id": row["resource_id"],
                    "format": row.get("format", "csv"),
                    "extras": json.dumps(row["extras"]),
                }
            )
    return str(path)


def _extras(parsing_table: str | None, parquet_url: str | None = None, error: str | None = None):
    return {
        "analysis:parsing:parsing_table": parsing_table,
        "analysis:parsing:parquet_url": parquet_url,
        "analysis:parsing:error": error,
    }


async def _seed_parsed_resource(db, fake_check, url: str) -> dict:
    """A resource in catalog, its parsing check, its tables_index entry and its table"""
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


async def _status(db, resource_id: str) -> str | None:
    return await db.fetchval("SELECT status FROM catalog WHERE resource_id = $1", resource_id)


async def test_backfill_parquet_enqueues_from_udata_extras(
    clean_db, db, fake_check, mocker, tmp_path
):
    """The parsing check is used even when a later crawl without parsing is the last check"""
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    url = "https://example.com/eligible.csv"
    check = await _seed_parsed_resource(db, fake_check, url)
    resource_id = check["resource_id"]
    # a later crawl, content unchanged: no parsing_table on the last check
    await fake_check(resource_id=resource_id, url=url)
    catalog = _write_catalog(
        tmp_path / "catalog.csv",
        [{"resource_id": resource_id, "extras": _extras(check["parsing_table"])}],
    )

    summary = await _backfill(catalog_file=catalog, dry_run=True)
    assert summary["enqueued"] == 1
    assert summary["total_lines"] == 4
    mock_enqueue.assert_not_called()
    assert await _status(db, resource_id) is None

    summary = await _backfill(catalog_file=catalog, dry_run=False)
    assert summary["enqueued"] == 1
    assert summary["skipped"] == {}
    mock_enqueue.assert_called_once()
    assert mock_enqueue.call_args.args[0] is export_parquet
    assert mock_enqueue.call_args.kwargs["_priority"] == "low"
    assert mock_enqueue.call_args.kwargs["check"]["id"] == check["id"]
    table = mock_enqueue.call_args.kwargs["table"]
    assert table.table_name == check["parsing_table"]
    assert table.dataset_id == DATASET_ID
    assert list(table.inspection["columns"]) == ["b", "a"]
    assert await _status(db, resource_id) == BACKFILL_STATUS


@pytest.mark.parametrize(
    "row",
    [
        {"extras": _extras("table", parquet_url="https://example.org/file.parquet")},
        {"extras": _extras("table", error="Parsing failed")},
        {"extras": _extras(None)},
        {"extras": {}},
        {"extras": _extras("table"), "archived": "True"},
        {"extras": _extras("table"), "format": "parquet"},
    ],
)
async def test_backfill_parquet_filters_on_catalog(clean_db, mocker, tmp_path, row):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    catalog = _write_catalog(tmp_path / "catalog.csv", [{"resource_id": str(uuid.uuid4()), **row}])

    summary = await _backfill(catalog_file=catalog)

    assert summary["candidates"] == 0
    mock_enqueue.assert_not_called()


@pytest.mark.parametrize(
    "case",
    ["not_in_catalog", "busy", "table_mismatch", "no_matching_check", "already_exported_locally"],
)
async def test_backfill_parquet_skips_on_local_state(
    clean_db, db, fake_check, mocker, tmp_path, case
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    check = await _seed_parsed_resource(db, fake_check, "https://example.com/skipped.csv")
    resource_id = check["resource_id"]
    parsing_table = check["parsing_table"]
    if case == "not_in_catalog":
        await db.execute("UPDATE catalog SET deleted = TRUE WHERE resource_id = $1", resource_id)
    elif case == "busy":
        await db.execute(
            "UPDATE catalog SET status = 'ANALYSING_CSV' WHERE resource_id = $1", resource_id
        )
    elif case == "table_mismatch":
        parsing_table = "an_older_table"
    elif case == "no_matching_check":
        await db.execute("UPDATE checks SET parsing_table = NULL WHERE id = $1", check["id"])
    elif case == "already_exported_locally":
        await db.execute(
            "UPDATE checks SET parquet_url = 'https://example.org/file.parquet' WHERE id = $1",
            check["id"],
        )
    catalog = _write_catalog(
        tmp_path / "catalog.csv",
        [{"resource_id": resource_id, "extras": _extras(parsing_table)}],
    )

    summary = await _backfill(catalog_file=catalog)

    assert summary["enqueued"] == 0
    assert summary["skipped"] == {case: 1}
    mock_enqueue.assert_not_called()


async def test_backfill_parquet_limit_follows_resource_id_order(
    clean_db, db, fake_check, mocker, tmp_path
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    checks = [
        await _seed_parsed_resource(db, fake_check, f"https://example.com/{i}.csv")
        for i in range(2)
    ]
    first = min(checks, key=lambda c: c["resource_id"])
    catalog = _write_catalog(
        tmp_path / "catalog.csv",
        [
            {"resource_id": c["resource_id"], "extras": _extras(c["parsing_table"])}
            for c in reversed(sorted(checks, key=lambda c: c["resource_id"]))
        ],
    )

    summary = await _backfill(catalog_file=catalog, limit=1)

    assert summary["enqueued"] == 1
    mock_enqueue.assert_called_once()
    assert mock_enqueue.call_args.kwargs["check"]["id"] == first["id"]


async def test_backfill_parquet_releases_lock_when_enqueue_fails(
    clean_db, db, fake_check, mocker, tmp_path
):
    mocker.patch("udata_hydra.utils.queue.enqueue", side_effect=ConnectionError("redis down"))
    check = await _seed_parsed_resource(db, fake_check, "https://example.com/redis-down.csv")
    catalog = _write_catalog(
        tmp_path / "catalog.csv",
        [{"resource_id": check["resource_id"], "extras": _extras(check["parsing_table"])}],
    )

    with pytest.raises(ConnectionError):
        await _backfill(catalog_file=catalog)

    assert await _status(db, check["resource_id"]) is None


async def test_backfill_parquet_downloads_catalog(
    clean_db, db, fake_check, mocker, rmock, tmp_path
):
    mock_enqueue = mocker.patch("udata_hydra.utils.queue.enqueue")
    check = await _seed_parsed_resource(db, fake_check, "https://example.com/downloaded.csv")
    catalog = _write_catalog(
        tmp_path / "catalog.csv",
        [{"resource_id": check["resource_id"], "extras": _extras(check["parsing_table"])}],
    )
    catalog_url = "https://example.com/catalog"
    with open(catalog, "rb") as fd:
        rmock.get(catalog_url, status=200, body=fd.read())

    summary = await _backfill(catalog_url=catalog_url)

    assert summary["enqueued"] == 1
    mock_enqueue.assert_called_once()

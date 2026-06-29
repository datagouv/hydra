import hashlib
import io
import json
import os
from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from slugify import slugify

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats import Parquet
from udata_hydra.utils import ParseException, storage_path

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "check_kwargs",
    [
        {"url": "http://example.com/file.parquet"},
        {"headers": {"content-type": "application/vnd.apache.parquet"}},
    ],
)
async def test_analyse_parquet(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
    check_kwargs,
):
    check = await fake_check(**check_kwargs)
    url = check["url"]
    df = pd.DataFrame(
        {
            "name": ["Marie", "Paul", "Léa", "Pierre"],
            "score": [1.2, 3.4, 5.6, 7.8],
            "decompte": [98, -76, -5, 0],
            "is_true": [True, False, False, True],
            "birth": [
                datetime.strptime("1996-02-13", "%Y-%m-%d"),
                datetime.strptime("2000-01-28", "%Y-%m-%d"),
                datetime.strptime("2005-11-10", "%Y-%m-%d"),
                datetime.strptime("2018-01-31", "%Y-%m-%d"),
            ],
            "stamp": [
                datetime.fromisoformat("2018-07-01T01:53:26.972000+00:00"),
                datetime.fromisoformat("2020-10-07T11:53:26.972000+00:00"),
                datetime.fromisoformat("1998-01-31T21:53:26.972000-04:00"),
                datetime.fromisoformat("2022-12-31T11:33:26.972000+02:00"),
            ],
            "liste": [
                [1, 2],
                [-3, 2],
                [6, -2],
                [-18, -17],
            ],
            "dicts": [
                {"a": 1},
                {"b": 2},
                {"c": 3},
                {"d": 4, "e": 5},
            ],
            "bina": [
                b"\x01\x01\x00",
                b"\x00\x00\x8c",
                b"\x01\x9e\xd1",
                b"\xb7\xd4\x00",
            ],
        }
    )
    expected_types = {
        "name": {"python": "string", "pg": "character varying"},
        "score": {"python": "float", "pg": "double precision"},
        "decompte": {"python": "int", "pg": "bigint"},
        "is_true": {"python": "bool", "pg": "boolean"},
        "birth": {"python": "datetime", "pg": "timestamp without time zone"},
        "stamp": {"python": "datetime_aware", "pg": "timestamp with time zone"},
        "liste": {"python": "json", "pg": "json"},
        "dicts": {"python": "json", "pg": "json"},
        "bina": {"python": "binary", "pg": "bytea"},
    }
    rmock.get(url, status=200, body=df.to_parquet())
    file = await helpers.download_from_check(check, Parquet)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        table = await file.analyse(check=check)
    assert table is not None
    # checking check result
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] == table.table_name
    assert res["parsing_error"] is None

    # checking table content
    rows = list(await db.fetch(f'SELECT * FROM "{table.table_name}"'))
    assert len(rows) == len(df)
    pgtypes = await db.fetchrow(
        "SELECT "
        + ", ".join([f"pg_typeof({col}) as {col}" for col in expected_types.keys()])
        + f' FROM "{table.table_name}"'
    )

    # checking analysis
    res = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    inspection = json.loads(res["csv_detective"])
    assert inspection["total_lines"] == len(df)
    assert inspection["header"] == list(df.columns)

    for col, types in expected_types.items():
        assert pgtypes[col] == types["pg"]
        assert inspection["columns"][col]["python_type"] == types["python"]


def _parquet_file(df: pd.DataFrame) -> pq.ParquetFile:
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    return pq.ParquetFile(buffer)


async def test_parquet_to_db_rejects_too_long_column_name(fake_check):
    """Reject parquet files whose column names exceed Postgres NAMEDATALEN."""
    inspection = {
        "columns": {"abcdefghijk": {"python_type": "int"}},
        "total_lines": 1,
    }
    check = await fake_check()
    with (
        patch("udata_hydra.config.NAMEDATALEN", 10),
        patch("udata_hydra.data_formats.data_format.os.path.getsize", return_value=10),
    ):
        file = Parquet(file_name="file.parquet", inspection=inspection)
        with pytest.raises(ParseException) as exc:
            await file.to_db(check=check)
    assert exc.value.step == "scan_column_names"


async def test_parquet_to_db_copy_failure_raises_parse_exception(db, clean_db, fake_check, mocker):
    """Wrap PostgreSQL COPY failures in a ParseException for consistent error handling."""
    check = await fake_check()
    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()

    await db.execute(f'CREATE TABLE "{table_name}" (__id SERIAL PRIMARY KEY, val text)')
    await db.execute(f"INSERT INTO \"{table_name}\" (val) VALUES ('original-data')")

    def make_failing_iter(*args, **kwargs):
        def _gen():
            raise RuntimeError("simulated copy failure")
            yield

        return _gen()

    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db._iter_parquet_rows",
        make_failing_iter,
    )

    inspection = {
        "columns": {"name": {"python_type": "string", "format": set()}},
        "total_lines": 1,
    }
    df = pd.DataFrame({"name": ["test"]})
    with NamedTemporaryFile(suffix=".parquet", dir=storage_path("")) as fp:
        df.to_parquet(fp.name)
        file = Parquet(file_name=os.path.basename(fp.name), inspection=inspection)
        with pytest.raises(ParseException) as exc:
            await file.to_db(check=check)

    assert exc.value.step == "copy_records_to_table"

    res = await db.fetch(f'SELECT * FROM "{table_name}"')
    assert len(res) == 1
    assert res[0]["val"] == "original-data"

    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_parquet_to_db_create_table_failure_raises_parse_exception(
    db, clean_db, fake_check, mocker
):
    """Wrap CREATE TABLE failures in a ParseException for consistent error handling."""
    check = await fake_check()
    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()

    await db.execute(f'CREATE TABLE "{table_name}" (__id SERIAL PRIMARY KEY, val text)')
    await db.execute(f"INSERT INTO \"{table_name}\" (val) VALUES ('original-data')")

    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db.compute_create_table_query",
        return_value="NOT A VALID SQL STATEMENT",
    )

    inspection = {
        "columns": {"name": {"python_type": "string", "format": set()}},
        "total_lines": 1,
    }
    df = pd.DataFrame({"name": ["test"]})
    with NamedTemporaryFile(suffix=".parquet", dir=storage_path("")) as fp:
        df.to_parquet(fp.name)
        file = Parquet(file_name=os.path.basename(fp.name), inspection=inspection)
        with pytest.raises(ParseException) as exc:
            await file.to_db(check=check)

    assert exc.value.step == "create_table_query"

    res = await db.fetch(f'SELECT * FROM "{table_name}"')
    assert len(res) == 1
    assert res[0]["val"] == "original-data"

    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')


async def test_parquet_to_db_indexes_renamed(db, clean_db, fake_check):
    """Indexes created with shadow table names are renamed to the real table name."""
    check = await fake_check()
    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()

    df = pd.DataFrame(
        {"name": ["Alice", "Bob"], "score": [100, 90], "category": ["Science", "Arts"]}
    )
    inspection = {
        "columns": {
            "name": {"python_type": "string", "format": set()},
            "score": {"python_type": "int", "format": set()},
            "category": {"python_type": "string", "format": set()},
        },
        "total_lines": 2,
    }
    table_indexes = {"name": "index", "category": "index"}
    with NamedTemporaryFile(suffix=".parquet", dir=storage_path("")) as fp:
        df.to_parquet(fp.name)
        file = Parquet(file_name=os.path.basename(fp.name), inspection=inspection)
        table = await file.to_db(check=check, table_indexes=table_indexes)

    assert table.table_name == table_name

    rows = await db.fetch(
        "SELECT indexname FROM pg_indexes WHERE tablename = $1 AND schemaname = $2",
        table_name,
        config.DATABASE_SCHEMA,
    )
    index_names = [r["indexname"] for r in rows]

    assert not any(name.startswith(f"{table_name}_s") for name in index_names), (
        f"Index names still contain shadow suffix: {index_names}"
    )

    for col in ("name", "category"):
        expected = f"{table_name}_{slugify(col)}_idx"
        assert expected in index_names, f"Expected index {expected} not found in {index_names}"

    res = list(await db.fetch(f'SELECT * FROM "{table_name}" ORDER BY name'))
    assert len(res) == 2
    assert res[0]["name"] == "Alice"
    assert res[1]["name"] == "Bob"

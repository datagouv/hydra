import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
from udata_hydra.data_formats import CsvLike, Parquet
from udata_hydra.data_formats.table.to_parquet import DEFAULT_PARQUET_FILENAME
from udata_hydra.db import quote_ident
from udata_hydra.utils.casting import iter_tabular_rows

pytestmark = pytest.mark.asyncio


def _reference_table_from_csv(file_path: Path, inspection: dict) -> pa.Table:
    """Build an Arrow table from a tabular file for assertions against ``db_to_parquet`` output.

    Uses ``iter_tabular_rows`` (``cast_json=False``) and ``PYTHON_TYPE_TO_PA`` for schema alignment.
    """
    rows = list(iter_tabular_rows(file_path, inspection, cast_json=False))
    columns = inspection["columns"]
    return pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in rows],
        schema=pa.schema(
            [pa.field(c, PYTHON_TYPE_TO_PA[columns[c]["python_type"]]) for c in columns]
        ),
    )


@pytest.mark.parametrize(
    "file_and_count",
    (
        ("catalog.csv", 2),
        ("catalog.xls", 2),
        ("catalog.xlsx", 2),
    ),
)
async def test_reference_table_from_csv(file_and_count):
    """Catalog fixtures: typed Arrow table row count and Parquet serialization for CSV / XLS / XLSX."""
    filename, expected_count = file_and_count
    file = CsvLike(file_name=f"tests/data/{filename}")
    inspection = await file.inspect()

    table = _reference_table_from_csv(file.path, inspection)
    assert table.num_rows == expected_count

    fake_file = BytesIO()
    pq.write_table(table, fake_file)


async def test_db_to_parquet(clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", True)
    check = await fake_check()
    file = CsvLike(file_name="tests/data/catalog.csv")
    inspection = await file.inspect()

    table = await file.to_db(check=check)
    with patch(
        # wraps because we don't want to change the behaviour, just to know it's been called
        "udata_hydra.data_formats.table.to_parquet.pa.table",
        wraps=pa.table,
    ) as pa_table_func:
        parquet = await table.to_parquet()
    assert parquet is not None
    table_from_db = pq.ParquetFile(parquet.path).read()

    table_from_csv = _reference_table_from_csv(file.path, inspection)

    assert table_from_db.num_rows == table_from_csv.num_rows
    assert table_from_db.to_pydict() == table_from_csv.to_pydict()
    # can't compare with table_from_db.schema as the reload can make the types differ (date32 VS date64 for instance)
    assert pa_table_func.call_args.kwargs["schema"] == table_from_csv.schema
    parquet.path.unlink()


async def test_db_to_parquet_embeds_latest_tables_index_entry(clean_db, fake_check, db, mocker):
    """The parquet carries the csv-detective inspection and ingestion date of the latest
    `tables_index` entry of its table, so that a reader of the file alone (api-tabular on DuckDB)
    can serve the same profile. A previous ingestion of the same url leaves a stale entry."""
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", True)
    check = await fake_check()
    file = CsvLike(file_name="tests/data/catalog.csv")
    await file.inspect()
    await file.to_db(check=check)
    table = await file.to_db(check=check)

    parquet = await table.to_parquet()
    assert parquet is not None

    metadata = pq.read_schema(parquet.path).metadata
    entries = await db.fetch(
        "SELECT csv_detective, created_at FROM tables_index "
        "WHERE parsing_table = $1 ORDER BY created_at DESC",
        table.table_name,
    )
    assert len(entries) == 2
    latest = entries[0]
    embedded = json.loads(metadata[b"csv_detective"])
    assert embedded == json.loads(latest["csv_detective"])
    assert embedded["profile"].keys() == file.inspection["columns"].keys()
    assert datetime.fromisoformat(metadata[b"created_at"].decode()) == latest["created_at"]
    parquet.path.unlink()


@pytest.mark.parametrize("db_to_parquet_flag", (False, True))
async def test_export_db_to_parquet(fake_check, mocker, db_to_parquet_flag, clean_db):
    csv_file = CsvLike(file_name="tests/data/catalog.csv")
    check = await fake_check()
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", db_to_parquet_flag)

    async def run_export() -> Parquet | None:
        await csv_file.inspect()
        table = await csv_file.to_db(check=check)
        return await table.to_parquet()

    if not db_to_parquet_flag:
        with patch("udata_hydra.data_formats.table.to_parquet.db_to_parquet") as conversion:
            assert await run_export() is None
            conversion.assert_not_called()
            assert not Path(DEFAULT_PARQUET_FILENAME).exists()
    else:
        parquet = await run_export()
        assert isinstance(parquet, Parquet)
        parquet.path.unlink()


async def test_quote_ident_escapes_double_quotes():
    """quote_ident must double embedded double quotes so a hostile column name
    cannot break out of the identifier position in a generated SELECT."""
    assert quote_ident('foo"bar') == '"foo""bar"'
    assert quote_ident('odd"name()') == '"odd""name()"'
    assert quote_ident("plain") == '"plain"'


async def test_db_to_parquet_escapes_quoted_column_name(clean_db, fake_check, mocker, tmp_path):
    """A column name containing a double quote must be exported safely, i.e. the
    quoting must hold so the name is treated as a literal identifier, not SQL."""
    csv = tmp_path / "hostile.csv"
    csv.write_text('"a""odd""name()",val\n1,row1\n2,row2\n')
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", True)
    mocker.patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", 1)
    check = await fake_check()
    file = CsvLike(file_name=str(csv))
    await file.inspect()
    table = await file.to_db(check=check)
    parquet = await table.to_parquet()
    assert parquet is not None
    assert parquet.path.exists()
    parquet.path.unlink()

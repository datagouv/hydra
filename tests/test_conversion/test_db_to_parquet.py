from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from udata_hydra.data_formats import CsvLike, Parquet
from udata_hydra.data_formats.table.to_parquet import DEFAULT_PARQUET_FILENAME
from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
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
    file = CsvLike(path=f"tests/data/{filename}")
    inspection = await file.inspect()

    table = _reference_table_from_csv(file.path, inspection)
    assert table.num_rows == expected_count

    fake_file = BytesIO()
    pq.write_table(table, fake_file)


async def test_db_to_parquet(clean_db, fake_check, mocker):
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", True)
    mocker.patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", 1)
    check = await fake_check()
    file_path = Path("tests/data/catalog.csv")
    file = CsvLike(path=file_path)
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

    table_from_csv = _reference_table_from_csv(file_path, inspection)

    assert table_from_db.num_rows == table_from_csv.num_rows
    assert table_from_db.to_pydict() == table_from_csv.to_pydict()
    # can't compare with table_from_db.schema as the reload can make the types differ (date32 VS date64 for instance)
    assert pa_table_func.call_args.kwargs["schema"] == table_from_csv.schema
    Path(DEFAULT_PARQUET_FILENAME).unlink()


@pytest.mark.parametrize(
    "parquet_config",
    (
        (False, 1, False),  # DB_TO_PARQUET off → no export
        (True, 10, False),  # MIN_LINES_FOR_PARQUET = 10 (> total_lines) → skip
        (True, 1, True),  # DB_TO_PARQUET on, MIN_LINES = 1 → convert
        (True, 3, False),  # MIN_LINES_FOR_PARQUET = 3 (> fixture row count) → skip
    ),
)
async def test_export_db_to_parquet(fake_check, mocker, parquet_config, clean_db):
    csv_file = CsvLike(path="tests/data/catalog.csv")
    check = await fake_check()
    db_to_parquet_flag, min_lines_for_parquet_config, expected_conversion = parquet_config
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", db_to_parquet_flag)
    mocker.patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", min_lines_for_parquet_config)

    async def run_export() -> Parquet | None:
        await csv_file.inspect()
        table = await csv_file.to_db(check=check)
        return await table.to_parquet()

    if not expected_conversion:
        with patch(
            "udata_hydra.data_formats.table.to_parquet.db_to_parquet"
        ) as conversion:
            assert await run_export() is None
            conversion.assert_not_called()
            assert not Path(DEFAULT_PARQUET_FILENAME).exists()
    else:
        parquet = await run_export()
        assert isinstance(parquet, Parquet)
        Path(DEFAULT_PARQUET_FILENAME).unlink()

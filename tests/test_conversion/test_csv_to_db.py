import json
import os
from datetime import date, datetime, timedelta, timezone
from tempfile import NamedTemporaryFile

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Csv
from udata_hydra.utils.db import get_columns_with_indexes

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "line_expected",
    (
        # (int, float, string, bool), (__id, int, float, string, bool)
        ("1,1020.20,test,true", (1, 1, 1020.2, "test", True), ","),
        ('2,"1020,20",test,false', (1, 2, 1020.2, "test", False), ","),
        ("1;1020.20;test;true", (1, 1, 1020.2, "test", True), ";"),
        ("2;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0;1020,20;test;false", (1, 2, 1020.2, "test", False), ";"),
        ("2.0|1020,20|test|false", (1, 2, 1020.2, "test", False), "|"),
    ),
)
async def test_csv_to_db_simple_type_casting(db, line_expected, clean_db, fake_check):
    check = await fake_check()
    line, expected, separator = line_expected
    header = separator.join(["int", "float", "string", "bool"])
    with NamedTemporaryFile() as fp:
        fp.write(f"{header}\n{line}".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        inspection = await file.inspect()
        assert inspection["separator"] == separator
        table = await file.to_db(check=check)
    res = list(await db.fetch(f'SELECT * FROM "{table.table_name}"'))
    assert len(res) == 1
    cols = ["__id", "int", "float", "string", "bool"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


@pytest.mark.parametrize(
    "line_expected",
    (
        # (json, date, datetime, aware_datetime), (__id, json, date, datetime, aware_datetime)
        (
            '{"a": 1};31 décembre 2022;2022-31-12 12:00:00.92;2030-06-22 00:00:00.0028+02:00',
            (
                1,
                json.dumps({"a": 1}),
                date(2022, 12, 31),
                datetime(2022, 12, 31, 12, 0, 0, 920000),
                datetime(2030, 6, 22, 0, 0, 0, 2800, tzinfo=timezone(timedelta(seconds=7200))),
            ),
        ),
        (
            '[{"a": 1, "b": 2}];31st december 2022;12/31/2022 12:00:00;1996/06/22 10:20:10 GMT',
            (
                1,
                json.dumps([{"a": 1, "b": 2}]),
                date(2022, 12, 31),
                datetime(2022, 12, 31, 12, 0, 0),
                datetime(1996, 6, 22, 10, 20, 10, tzinfo=timezone.utc),
            ),
        ),
    ),
)
async def test_csv_to_db_complex_type_casting(db, line_expected, clean_db, fake_check):
    check = await fake_check()
    line, expected = line_expected
    with NamedTemporaryFile() as fp:
        fp.write(f"json;date;datetime;aware_datetime\n{line}".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check)
    res = list(await db.fetch(f'SELECT * FROM "{table.table_name}"'))
    assert len(res) == 1
    cols = ["__id", "json", "date", "datetime", "aware_datetime"]
    assert dict(res[0]) == {k: v for k, v in zip(cols, expected)}


async def test_basic_sql_injection(db, clean_db, fake_check):
    check = await fake_check()
    # tries to execute
    # CREATE TABLE table_name("int" integer, "col_name" text);DROP TABLE toto;--)
    injection = 'col_name" text);DROP TABLE toto;--'
    with NamedTemporaryFile() as fp:
        # we enough columns so that the ";" is not considered as separator by csv-detective
        fp.write(f"int,{injection},col1,col2\n1,test,2,3".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check)
    res = await db.fetchrow(f'SELECT * FROM "{table.table_name}"')
    assert res[injection] == "test"


async def test_percentage_column(db, clean_db, fake_check):
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write("int,% mon pourcent\n1,test".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check)
    res = await db.fetchrow(f'SELECT * FROM "{table.table_name}"')
    assert res["% mon pourcent"] == "test"


async def test_reserved_column_name(db, clean_db, fake_check):
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write("int,xmin\n1,test".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check)
    res = await db.fetchrow(f'SELECT * FROM "{table.table_name}"')
    assert res["xmin__hydra_renamed"] == "test"
    assert table.inspection["columns_mapping"] == {"xmin": "xmin__hydra_renamed"}


LONG_COL = "Nombre de logements sociaux conventionnés livrés au cours de l'année 2023"
LONG_COL_ACCENTS = "Répartition des émissions de gaz à effet de serre par secteur d'activité en €"


@pytest.mark.parametrize("long_col", (LONG_COL, LONG_COL_ACCENTS))
async def test_long_column_name(db, clean_db, fake_check, long_col):
    """Column names that don't fit in Postgres are truncated instead of failing the analysis."""
    assert len(long_col.encode("utf-8")) > 63
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write(f"int,{long_col}\n1,test".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check)

    db_col = table.inspection["columns_mapping"][long_col]
    assert len(db_col.encode("utf-8")) <= 63
    assert db_col.endswith("__col1")
    assert long_col.startswith(db_col[: -len("__col1")])
    res = await db.fetchrow(f'SELECT * FROM "{table.table_name}"')
    assert res[db_col] == "test"
    # the short column is left alone
    assert table.inspection["columns_mapping"].keys() == {long_col}


async def test_index_on_long_column_name(db, clean_db, fake_check):
    """The index name embeds a 32-char md5 table name, so it needs truncating too."""
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write(f"int,{LONG_COL}\n1,test".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check, table_indexes={LONG_COL: "index"})

    db_col = table.inspection["columns_mapping"][LONG_COL]
    indexes = await get_columns_with_indexes(table.table_name)
    assert indexes
    assert db_col in [idx["column_name"] for idx in indexes]
    for idx in indexes:
        assert len(idx["index_name"].encode("utf-8")) <= 63


async def test_indexes_on_columns_sharing_a_prefix(db, clean_db, fake_check):
    """Two long column names slugify to the same prefix: their indexes must not collide."""
    prefix = "Nombre de logements sociaux conventionnés livrés au cours de l'année "
    first, second = prefix + "2023", prefix + "2024"
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write(f"int,{first},{second}\n1,a,b".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check, table_indexes={first: "index", second: "index"})

    mapping = table.inspection["columns_mapping"]
    indexes = await get_columns_with_indexes(table.table_name)
    assert indexes
    indexed_columns = [idx["column_name"] for idx in indexes]
    assert mapping[first] in indexed_columns
    assert mapping[second] in indexed_columns


async def test_index_on_reserved_column_name(db, clean_db, fake_check):
    """table_indexes keys are source names: they must go through the mapping."""
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write(b"int,xmin\n1,test")
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check, table_indexes={"xmin": "index"})

    indexes = await get_columns_with_indexes(table.table_name)
    assert indexes
    assert "xmin__hydra_renamed" in [idx["column_name"] for idx in indexes]


async def test_debug_insert_with_renamed_columns(db, clean_db, fake_check):
    """The row-by-row debug path inserts into the actual PG columns, not the source names."""
    check = await fake_check()
    with NamedTemporaryFile() as fp:
        fp.write(f"xmin,{LONG_COL}\n1,test".encode("utf-8"))
        fp.seek(0)
        file = Csv(file_name=os.path.basename(fp.name), resource_id=RESOURCE_ID)
        await file.inspect()
        table = await file.to_db(check=check, debug_insert=True)

    db_col = table.inspection["columns_mapping"][LONG_COL]
    res = await db.fetchrow(f'SELECT * FROM "{table.table_name}"')
    assert res[db_col] == "test"
    assert res["xmin__hydra_renamed"] == 1

import json
from datetime import date, datetime, timedelta, timezone
import os
from tempfile import NamedTemporaryFile

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Csv

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

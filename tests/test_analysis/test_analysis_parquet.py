import hashlib
import json
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from udata_hydra.analysis.parquet import analyse_parquet

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
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    parquet_file_content = pd.DataFrame(
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
    ).to_parquet()
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
    rmock.get(url, status=200, body=parquet_file_content)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        await analyse_parquet(check=check)

    # checking check result
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] == table_name
    assert res["parsing_error"] is None

    # checking table content
    rows = list(await db.fetch(f'SELECT * FROM "{table_name}"'))
    assert len(rows) == 4
    pgtypes = await db.fetchrow(
        "SELECT "
        + ", ".join([f"pg_typeof({col}) as {col}" for col in expected_types.keys()])
        + f' FROM "{table_name}"'
    )

    # checking analysis
    res = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    inspection = json.loads(res["csv_detective"])

    for col, types in expected_types.items():
        assert pgtypes[col] == types["pg"]
        assert inspection["columns"][col]["python_type"] == types["python"]

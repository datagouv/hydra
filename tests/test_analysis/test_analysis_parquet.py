import json
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from csv_detective import validate_then_detect
from csv_detective.explore_csv import detect_formats

from tests.conftest import job_states
from udata_hydra.analysis import helpers
from udata_hydra.analysis.exports import export_parquet
from udata_hydra.data_formats import Parquet, Table
from udata_hydra.db.resource_job_status import ResourceJobStatus
from udata_hydra.utils import ParseException

pytestmark = pytest.mark.asyncio


def _producer_dataframe() -> pd.DataFrame:
    """A parquet file of a producer, with a column of each type hydra reads."""
    return pd.DataFrame(
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
            # strings that csv-detective recognises, as in a CSV
            "code_postal": ["75001", "13002", "69003", "33000"],
        }
    )


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
    job_status_snapshots,
):
    check = await fake_check(**check_kwargs)
    url = check["url"]
    df = _producer_dataframe()
    # the python types and formats of csv-detective, which reads the parquet types for both
    expected_types = {
        "name": {"python": "string", "format": "string", "pg": "character varying"},
        "score": {"python": "float", "format": "float", "pg": "double precision"},
        "decompte": {"python": "int", "format": "int", "pg": "bigint"},
        "is_true": {"python": "bool", "format": "bool", "pg": "boolean"},
        "birth": {
            "python": "datetime",
            "format": "datetime_naive",
            "pg": "timestamp without time zone",
        },
        "stamp": {
            "python": "datetime",
            "format": "datetime_aware",
            "pg": "timestamp with time zone",
        },
        "liste": {"python": "json", "format": "json", "pg": "json"},
        "dicts": {"python": "json", "format": "json", "pg": "json"},
        "bina": {"python": "binary", "format": "binary", "pg": "bytea"},
        "code_postal": {"python": "string", "format": "code_postal", "pg": "character varying"},
    }
    rmock.get(url, status=200, body=df.to_parquet())
    file = await helpers.download_from_check(check, Parquet)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        table = await file.analyse(check=check)
    assert table is not None
    assert job_states(job_status_snapshots, "parquet") == ["ANALYSING_PARQUET", "INSERTING_IN_DB"]
    assert await ResourceJobStatus.for_resource(check["resource_id"]) == {}
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
        assert inspection["columns"][col]["format"] == types["format"]


async def _analyse_producer_parquet(
    rmock, fake_check, df: pd.DataFrame | None = None
) -> tuple[dict, Table]:
    check = await fake_check(url="http://example.com/file.parquet")
    body = (df if df is not None else _producer_dataframe()).to_parquet()
    rmock.get(check["url"], status=200, body=body)
    file = await helpers.download_from_check(check, Parquet)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        table = await file.analyse(check=check)
    assert table is not None
    return check, table


async def test_analyse_parquet_enqueues_the_parquet_export(
    setup_catalog, rmock, fake_check, produce_mock, mocker
):
    """The parquet file of a producer is exported again from its table, like a CSV."""
    mock_enqueue = mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    with patch("udata_hydra.config.DB_TO_PARQUET", True):
        check, table = await _analyse_producer_parquet(rmock, fake_check)

    mock_enqueue.assert_called_once()
    assert mock_enqueue.call_args.args[0] is export_parquet
    kwargs = mock_enqueue.call_args.kwargs
    assert kwargs["table"].table_name == table.table_name
    assert kwargs["check"]["id"] == check["id"]
    assert kwargs["_priority"] == "low"
    # the analysis clears the same job key when it ends: the export state must outlive it
    status = await ResourceJobStatus.for_resource(check["resource_id"])
    assert status["parquet"]["state"] == "CONVERTING_TO_PARQUET"


async def test_analyse_parquet_without_db_to_parquet(
    setup_catalog, rmock, fake_check, produce_mock, mocker
):
    mock_enqueue = mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    with patch("udata_hydra.config.DB_TO_PARQUET", False):
        check, _ = await _analyse_producer_parquet(rmock, fake_check)

    mock_enqueue.assert_not_called()
    assert await ResourceJobStatus.for_resource(check["resource_id"]) == {}


async def test_parquet_of_a_producer_exported_from_its_table(
    setup_catalog, db, rmock, fake_check, produce_mock, mocker
):
    """The values of the producer file survive the table, and csv-detective's analysis of it,
    formats and profile included, is stored and embedded in the file api-tabular reads."""
    mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    check, table = await _analyse_producer_parquet(rmock, fake_check)
    res = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    stored = json.loads(res["csv_detective"])
    with patch("udata_hydra.config.DB_TO_PARQUET", True):
        parquet = await table.to_parquet()
    assert parquet is not None

    exported = pq.read_table(parquet.path)
    parquet.path.unlink()
    # the inspection of the producer file, which api-tabular serves as the profile
    embedded = json.loads(exported.schema.metadata[b"csv_detective"])
    assert embedded["columns"]["stamp"]["format"] == "datetime_aware"
    assert embedded["columns"]["code_postal"]["format"] == "code_postal"
    assert embedded["total_lines"] == 4
    for inspection in (stored, embedded):
        assert inspection["formats"]["code_postal"] == ["code_postal"]
        assert inspection["profile"]["code_postal"]["nb_distinct"] == 4
        assert all(
            inspection["profile"]["decompte"][stat] is not None
            for stat in ["min", "max", "mean", "std"]
        )
    assert exported.to_pydict() == {
        "name": ["Marie", "Paul", "Léa", "Pierre"],
        "score": [1.2, 3.4, 5.6, 7.8],
        "decompte": [98, -76, -5, 0],
        "is_true": [True, False, False, True],
        "birth": [
            datetime(1996, 2, 13),
            datetime(2000, 1, 28),
            datetime(2005, 11, 10),
            datetime(2018, 1, 31),
        ],
        "stamp": [
            datetime(2018, 7, 1, 1, 53, 26, 972000, tzinfo=timezone.utc),
            datetime(2020, 10, 7, 11, 53, 26, 972000, tzinfo=timezone.utc),
            datetime(1998, 2, 1, 1, 53, 26, 972000, tzinfo=timezone.utc),
            datetime(2022, 12, 31, 9, 33, 26, 972000, tzinfo=timezone.utc),
        ],
        # json columns are text in the table, as in the parquet files exported from a CSV
        "liste": ["[1, 2]", "[-3, 2]", "[6, -2]", "[-18, -17]"],
        # pandas reads a struct column with every key of the column, and its ints as floats
        "dicts": [
            '{"a": 1.0, "b": null, "c": null, "d": null, "e": null}',
            '{"a": null, "b": 2.0, "c": null, "d": null, "e": null}',
            '{"a": null, "b": null, "c": 3.0, "d": null, "e": null}',
            '{"a": null, "b": null, "c": null, "d": 4.0, "e": 5.0}',
        ],
        "bina": [b"\x01\x01\x00", b"\x00\x00\x8c", b"\x01\x9e\xd1", b"\xb7\xd4\x00"],
        "code_postal": ["75001", "13002", "69003", "33000"],
    }


async def test_parquet_analysed_again_validates_the_previous_inspection(
    setup_catalog, db, rmock, fake_check, produce_mock, mocker
):
    """A parquet file analysed again is validated against its previous inspection, like a CSV."""
    mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    check, _ = await _analyse_producer_parquet(rmock, fake_check)
    rmock.get(check["url"], status=200, body=_producer_dataframe().to_parquet())
    with (
        patch(
            # wraps because we don't want to change the behaviour, just to know it's been called
            "udata_hydra.analysis.helpers.validate_then_detect",
            wraps=validate_then_detect,
        ) as mock_func,
        # only called when the validation fails and csv-detective detects the file from scratch
        patch("csv_detective.explore_csv.detect_formats", wraps=detect_formats) as mock_detect,
    ):
        file = await helpers.download_from_check(check, Parquet)
        with patch("udata_hydra.config.PARQUET_TO_DB", True):
            await file.analyse(check=check)
        mock_func.assert_called_once()
        mock_detect.assert_not_called()

    res = await db.fetch(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1 ORDER BY created_at DESC",
        check["resource_id"],
    )
    assert len(res) == 2
    latest = json.loads(res[0]["csv_detective"])
    assert latest["columns"]["code_postal"]["format"] == "code_postal"
    assert latest["profile"]["code_postal"]["nb_distinct"] == 4


@pytest.mark.parametrize(
    "df,legacy_types",
    [
        # a format csv-detective doesn't know (datetime), so the validation stops early
        (
            _producer_dataframe(),
            {
                "name": "string",
                "score": "float",
                "decompte": "int",
                "is_true": "bool",
                "birth": "datetime",
                "stamp": "datetime_aware",
                "liste": "json",
                "dicts": "json",
                "bina": "binary",
                "code_postal": "string",
            },
        ),
        # only known formats, so the validation fails on the missing engine
        (
            pd.DataFrame({"decompte": [1, 2], "code_postal": ["75001", "13002"]}),
            {"decompte": "int", "code_postal": "string"},
        ),
    ],
)
async def test_parquet_with_a_legacy_inspection_detected_again(
    setup_catalog, db, rmock, fake_check, produce_mock, mocker, df, legacy_types
):
    """The parquet files analysed before csv-detective have an inspection made from their arrow
    types: the validation fails on it, and csv-detective detects the file again."""
    mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    check, _ = await _analyse_producer_parquet(rmock, fake_check, df)
    legacy_inspection = {
        "header": list(legacy_types),
        "columns": {col: {"format": t, "python_type": t} for col, t in legacy_types.items()},
        "total_lines": len(df),
    }
    await db.execute(
        "UPDATE tables_index SET csv_detective = $1 WHERE resource_id = $2",
        json.dumps(legacy_inspection),
        check["resource_id"],
    )
    rmock.get(check["url"], status=200, body=df.to_parquet())
    with patch("csv_detective.explore_csv.detect_formats", wraps=detect_formats) as mock_detect:
        file = await helpers.download_from_check(check, Parquet)
        with patch("udata_hydra.config.PARQUET_TO_DB", True):
            await file.analyse(check=check)
        mock_detect.assert_called_once()

    res = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1 ORDER BY created_at DESC",
        check["resource_id"],
    )
    latest = json.loads(res["csv_detective"])
    assert latest["engine"] == "parquet"
    assert latest["columns"]["code_postal"]["format"] == "code_postal"
    assert latest["profile"]["code_postal"]["nb_distinct"] == len(df)


async def test_parquet_with_a_reserved_column_name_analysed_again(
    setup_catalog, db, rmock, fake_check, produce_mock, mocker
):
    """A column named like a Postgres system column is renamed in the table but not in the
    stored inspection: the previous inspection must still be found for it."""
    mocker.patch("udata_hydra.data_formats.parquet.queue.enqueue")
    df = pd.DataFrame({"xmin": [1, 2], "name": ["x", "y"]})
    check, _ = await _analyse_producer_parquet(rmock, fake_check, df)
    rmock.get(check["url"], status=200, body=df.to_parquet())
    file = await helpers.download_from_check(check, Parquet)
    with patch("udata_hydra.config.PARQUET_TO_DB", True):
        table = await file.analyse(check=check)
    assert table is not None

    rows = await db.fetch(f'SELECT xmin__hydra_renamed, name FROM "{table.table_name}"')
    assert [tuple(row) for row in rows] == [(1, "x"), (2, "y")]
    res = await db.fetch(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    assert len(res) == 2


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


async def test_parquet_to_db_copy_failure_raises_parse_exception(fake_check, mocker):
    """Wrap PostgreSQL COPY failures in a ParseException for consistent error handling."""
    check = await fake_check()
    mock_pool = mocker.MagicMock()
    mock_pool.execute = mocker.AsyncMock()
    mock_pool.copy_records_to_table = mocker.AsyncMock(
        side_effect=RuntimeError("copy failed"),
    )
    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db.context.pool",
        new=mocker.AsyncMock(return_value=mock_pool),
    )
    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db.ResourceJobStatus.set", new=mocker.AsyncMock()
    )
    inspection = {
        "columns": {"name": {"python_type": "string", "format": set()}},
        "total_lines": 1,
    }
    with patch("udata_hydra.data_formats.data_format.os.path.getsize", return_value=10):
        file = Parquet(file_name="file.parquet", inspection=inspection)
    with pytest.raises(ParseException) as exc:
        await file.to_db(check=check)
    assert exc.value.step == "copy_records_to_table"


async def test_parquet_to_db_create_table_failure_raises_parse_exception(fake_check, mocker):
    """Wrap CREATE TABLE failures in a ParseException for consistent error handling."""
    check = await fake_check()
    mock_pool = mocker.MagicMock()

    async def execute(q: str, *args: object) -> None:
        if "CREATE TABLE" in q.upper():
            raise RuntimeError("ddl failed")

    mock_pool.execute = mocker.AsyncMock(side_effect=execute)
    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db.context.pool",
        new=mocker.AsyncMock(return_value=mock_pool),
    )
    mocker.patch(
        "udata_hydra.data_formats.parquet.to_db.ResourceJobStatus.set", new=mocker.AsyncMock()
    )
    inspection = {
        "columns": {"name": {"python_type": "string", "format": set()}},
        "total_lines": 1,
    }
    with patch("udata_hydra.data_formats.data_format.os.path.getsize", return_value=10):
        file = Parquet(file_name="file.parquet", inspection=inspection)
    with pytest.raises(ParseException) as exc:
        await file.to_db(check=check)
    assert exc.value.step == "create_table_query"

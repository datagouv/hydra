import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import ClientSession
from asyncpg.exceptions import UndefinedTableError
from csv_detective import validate_then_detect
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.analysis.exports import export_geojson_pmtiles, export_parquet
from udata_hydra.analysis.helpers import download_from_check
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.data_formats import Csv, Geojson, Parquet, PMTiles, Table
from udata_hydra.db.check import Check
from udata_hydra.db.codec import parse_json_value
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


async def test_analyse_csv_on_catalog(
    setup_catalog, rmock, catalog_content, db, fake_check, produce_mock
):
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body=catalog_content)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] == table_name
    assert res["parsing_error"] is None
    rows = list(await db.fetch(f'SELECT * FROM "{table_name}"'))
    assert len(rows) == 2
    row = rows[0]
    assert row["id"] == RESOURCE_ID
    assert row["url"] == RESOURCE_URL
    res = await db.fetchrow("SELECT * from tables_index")
    inspection = parse_json_value(res["csv_detective"])
    assert all(k in inspection["columns"] for k in ["id", "url"])


@pytest.mark.slow
async def test_analyse_csv_big_file(setup_catalog, rmock, db, fake_check, produce_mock):
    """
    This test is slow because it parses a pretty big file.
    It's meant to act as a "canary in the coal mine": if performance degrades too much, you or the CI should feel it.
    You can deselect it by running `pytest -m "not slow"`.
    """
    TEST_CSV_FILE, EXPECTED_COUNT = ("20190618-annuaire-diagnostiqueurs.csv", 45522)

    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()

    csv_path = "tests/data" / Path(TEST_CSV_FILE)
    with csv_path.open("rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    count = await db.fetchrow(f'SELECT count(*) AS count FROM "{table_name}"')
    assert count["count"] == EXPECTED_COUNT
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = parse_json_value(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == EXPECTED_COUNT


async def test_error_reporting_csv_detective(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    rmock.get(url, status=200, body="".encode("utf-8"))

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert res["parsing_error"] == "csv_detective:Could not accurately retrieve headers position"
    assert res["parsing_finished_at"]


async def test_error_reporting_parsing(
    rmock, catalog_content, db, setup_catalog, fake_check, produce_mock
):
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(url, status=200, body="a,b,c\n1,2".encode("utf-8"))

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis attempt
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    res = await db.fetchrow("SELECT * FROM checks")
    assert res["parsing_table"] is None
    assert (
        res["parsing_error"]
        == "csv_detective:Number of columns is not even across the first 10 rows (detected separator: ,)."
    )
    assert res["parsing_finished_at"]
    with pytest.raises(UndefinedTableError):
        await db.execute(f'SELECT * FROM "{table_name}"')


async def test_analyse_csv_send_udata_webhook(
    setup_catalog, rmock, catalog_content, db, fake_check, udata_url
):
    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    rmock.put(udata_url, status=200)

    # Analyse the CSV
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}

    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("analysis:parsing:started_at")
    assert webhook.get("analysis:parsing:finished_at")
    assert webhook.get("analysis:parsing:parsing_table")
    assert webhook.get("analysis:parsing:error") is None
    for k in ["parquet_size", "parquet_url"]:
        assert webhook.get(f"analysis:parsing:{k}", False) is None


async def test_analyse_csv_enqueues_export_jobs_on_low_queue(
    mocker, setup_catalog, rmock, catalog_content, db, fake_check, produce_mock
):
    """Parquet export is scheduled on the low RQ queue (CSV geo export disabled)."""
    import asyncio

    recorded: list[tuple[object, str | None]] = []

    async def tracking_parquet_export(*args, **kwargs):
        pass

    mocker.patch("udata_hydra.analysis.exports.export_parquet", tracking_parquet_export)

    def capture_enqueue(fn, *args, **kwargs):
        recorded.append((fn, kwargs.get("_priority")))
        kwargs = dict(kwargs)
        kwargs.pop("_priority", None)
        kwargs.pop("_exception", None)
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            loop = asyncio.get_running_loop()
            return loop.run_until_complete(result)
        return result

    mocker.patch("udata_hydra.utils.queue.enqueue", capture_enqueue)

    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    with (
        patch("udata_hydra.config.DB_TO_PARQUET", True),
        patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", 1),
        patch("udata_hydra.config.DB_TO_GEOJSON", False),
    ):
        file = await download_from_check(check, Csv)
        await file.analyse(check=check)

    assert any(f is tracking_parquet_export and p == "low" for f, p in recorded)

    from udata_hydra.analysis.exports import export_geojson_pmtiles as exp_geo

    assert not any(f is exp_geo for f, _ in recorded)


@pytest.mark.parametrize(
    "forced_analysis",
    (
        (True, True),
        (False, False),
    ),
)
async def test_forced_analysis(
    setup_catalog,
    rmock,
    catalog_content,
    db,
    fake_check,
    forced_analysis,
    udata_url,
):
    force_analysis, table_exists = forced_analysis
    check = await fake_check(
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        detected_last_modified_at=datetime(2025, 4, 4, 0, 0, 0),
    )
    url = check["url"]
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    rmock.head(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
    )
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body="a,b,c\n1,2,3".encode("utf-8"),
        repeat=True,
    )
    rmock.put(udata_url, status=200, repeat=True)
    async with ClientSession() as session:
        await check_resource(
            url=url, resource=resource, session=session, force_analysis=force_analysis
        )

    # check that csv was indeed pushed to db
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert (table_name in [r["table_name"] for r in tables]) == table_exists

    # check whether udata was pinged
    if force_analysis:
        webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
        assert webhook.get("analysis:parsing:started_at")
        assert webhook.get("analysis:parsing:finished_at")
        assert webhook.get("analysis:parsing:parsing_table")
        assert webhook.get("analysis:parsing:error") is None
        for k in ["parquet_size", "parquet_url"]:
            assert webhook.get(f"analysis:parsing:{k}", False) is None
    else:
        assert ("PUT", URL(udata_url)) not in rmock.requests.keys()


def create_body(
    separator: str,
    header: list[str],
    rows: list[list[str]],
    encoding: str,
    **kwargs,
) -> bytes:
    return "\n".join(separator.join(cell for cell in row) for row in [header] + rows).encode(
        encoding
    )


default_kwargs = {
    "separator": ",",
    "header": ["a", "epci"],
    "rows": [["1", "13002526500013"], ["5", "38271817900023"]],
    "encoding": "utf-8",
    "columns": {
        "a": {"score": 1.0, "format": "int", "python_type": "int"},
        "epci": {"score": 1.5, "format": "siret", "python_type": "string"},
    },
    "header_row_idx": 0,
    "categorical": None,
    "formats": {"int": ["a"], "siret": ["epci"]},
    "columns_fields": None,
    "columns_labels": None,
    "profile": None,
}

python_type_to_sql = {
    "int": "INT",
    "string": "VARCHAR",
    "float": "FLOAT",
}


def create_analysis(scan: dict) -> dict:
    analysis = {
        k: scan[k]
        for k in [
            "encoding",
            "separator",
            "header_row_idx",
            "header",
            "columns",
            "categorical",
            "formats",
            "columns_fields",
            "columns_labels",
            "profile",
        ]
    }
    analysis["total_lines"] = len(scan["rows"])
    return analysis


@pytest.mark.parametrize(
    "_params",
    (
        # just a new row in the file, types haven't changed
        (
            default_kwargs,
            default_kwargs | {"rows": default_kwargs["rows"] + [["6", "21310555400017"]]},
            True,
        ),
        # separator changed
        (default_kwargs, default_kwargs | {"separator": ";"}, False),
        # columns changed
        (
            default_kwargs,
            default_kwargs
            | {
                "header": ["a", "ID_EPCI"],
                "columns": {
                    "a": {"score": 1.0, "format": "int", "python_type": "int"},
                    "ID_EPCI": {"score": 1.25, "format": "siret", "python_type": "string"},
                },
                "formats": {"int": ["a"], "siret": ["ID_EPCI"]},
            },
            False,
        ),
        # format changed
        (
            default_kwargs,
            default_kwargs
            | {
                "header": ["a", "b"],
                "rows": [["1", "2022-11-03"], ["5", "2025-11-02"]],
                "columns": {
                    "a": {"score": 1.0, "format": "int", "python_type": "int"},
                    "b": {"score": 1.0, "format": "date", "python_type": "date"},
                },
                "formats": {"int": ["a"], "date": ["b"]},
            },
            False,
        ),
    ),
)
async def test_validation(
    setup_catalog,
    rmock,
    db,
    fake_check,
    udata_url,
    _params,
    produce_mock,
):
    previous_scan_kwargs, current_scan_kwargs, is_valid = _params
    previous_analysis = create_analysis(previous_scan_kwargs)
    current_analysis = create_analysis(current_scan_kwargs)

    # set up check and url
    check = await fake_check(
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
    )
    url = check["url"]
    rmock.get(
        url,
        status=200,
        body=create_body(**current_scan_kwargs),
    )
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()

    # set up previous analysis and db insertion
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) VALUES($1, $2, $3, $4, $5)",
        table_name,
        previous_analysis,
        check.get("resource_id"),
        check.get("dataset_id"),
        check.get("url"),
    )
    await db.execute(
        f'CREATE TABLE "{table_name}"(__id serial PRIMARY KEY, '
        + ", ".join(
            f"{col} {python_type_to_sql[previous_analysis['columns'][col]['python_type']]}"
            for col in previous_analysis["columns"]
        )
        + ")"
    )
    await db.execute(
        f'INSERT INTO "{table_name}" ({", ".join(previous_analysis["header"])}) VALUES '
        + ", ".join(f"({','.join(row)})" for row in previous_scan_kwargs["rows"])
    )

    # run analysis
    with patch(
        # wraps because we don't want to change the behaviour, just to know it's been called
        "udata_hydra.data_formats.csv_like.validate_then_detect",
        wraps=validate_then_detect,
    ) as mock_func:
        file = await download_from_check(check, Csv)
        await file.analyse(check=check)
        mock_func.assert_called_once()

    # now we check what is inside csv_detective in tables_index
    res = await db.fetch(
        f"SELECT * from tables_index WHERE resource_id='{check['resource_id']}' "
        "ORDER BY created_at DESC"
    )
    assert len(res) == 2
    assert all(row["parsing_table"] == table_name for row in res)
    assert all(str(row["resource_id"]) == check["resource_id"] for row in res)
    assert all(row["url"] == check["url"] for row in res)
    assert all(row["dataset_id"] == check["dataset_id"] for row in res)
    assert all(row["deleted_at"] is None for row in res)  # Should be NULL for new records
    latest_analysis = parse_json_value(res[0]["csv_detective"])
    # check that latest analysis is in line with expectation
    for key in current_analysis.keys():
        if current_analysis[key] is not None:
            assert latest_analysis[key] == current_analysis[key]
        if not is_valid:
            # if valid, we have kept the exact same analysis, so None should remain
            assert latest_analysis[key] is not None
    # in all cases, profile should have been recreated
    assert latest_analysis["profile"] is not None


@pytest.mark.parametrize(
    "params",
    (
        ("col", False),
        ("çà€", True),
    ),
)
async def test_too_long_column_name(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
    params,
):
    col, has_non_ascii = params
    url = "http://example.com/csv"
    max_len = 10
    col_name = (col * ((max_len // len(col)) + 1))[: max_len if not has_non_ascii else max_len - 3]
    check = await fake_check(headers={"content-type": "application/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=f"{col_name},b,c\n1,2,3".encode("utf-8"),
        repeat=True,
    )
    # should fail because one column name is too long
    with patch("udata_hydra.config.NAMEDATALEN", max_len):
        file = await download_from_check(check, Csv)
        await file.analyse(check=check)
    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    # analysis failed
    assert updated_check["parsing_error"].startswith("scan_column_names:")
    # table was not created
    tables = await db.fetch(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public';"
    )
    assert table_name not in [r["table_name"] for r in tables]


@pytest.mark.parametrize(
    "db_to_parquet_enabled, expected_parquet_jobs",
    (
        pytest.param(True, 1, id="enabled"),
        pytest.param(False, 0, id="disabled"),
    ),
)
async def test_analyse_csv_parquet_export_enqueue(
    mocker,
    setup_catalog,
    rmock,
    catalog_content,
    fake_check,
    produce_mock,
    db_to_parquet_enabled,
    expected_parquet_jobs,
):
    """Parquet export enqueue respects DB_TO_PARQUET, uses low priority, skips geo export."""
    from udata_hydra.analysis.exports import export_geojson_pmtiles as exp_geo

    mock_enqueue = mocker.patch("udata_hydra.data_formats.csv_like.queue.enqueue")

    check = await fake_check(headers={"content-type": "text/csv"})
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)

    with (
        patch("udata_hydra.config.DB_TO_PARQUET", db_to_parquet_enabled),
        patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", 1),
        patch("udata_hydra.config.DB_TO_GEOJSON", False),
    ):
        file = await download_from_check(check, Csv)
        await file.analyse(check=check)

    parquet_jobs = [
        c for c in mock_enqueue.call_args_list if c.args and c.args[0] is export_parquet
    ]
    assert len(parquet_jobs) == expected_parquet_jobs
    assert not any(c.args and c.args[0] is exp_geo for c in mock_enqueue.call_args_list)

    if db_to_parquet_enabled:
        kw = parquet_jobs[0].kwargs
        assert kw["_priority"] == "low"
        assert kw["check"]["id"] == check["id"]
        assert "table" in kw


async def test_crash_after_db_insertion(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
):
    async def _crash(*args, **kwargs):
        raise Exception("BOOM")

    queued_parquet_kwargs: list[dict] = []

    def capture_enqueue(fn, *args, **kwargs):
        if fn is export_parquet:
            queued_parquet_kwargs.append(dict(kwargs))
        return MagicMock()

    check = await fake_check(headers={"content-type": "application/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=("a,b,c\n" + "1,2,3\n" * 200).encode("utf-8"),
        repeat=True,
    )
    with (
        patch("udata_hydra.config.DB_TO_PARQUET", True),
        patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", 1),
        patch("udata_hydra.data_formats.csv_like.queue.enqueue", side_effect=capture_enqueue),
    ):
        file = await download_from_check(check, Csv)
        await file.analyse(check=check)

    assert len(queued_parquet_kwargs) == 1
    job_kw = queued_parquet_kwargs[0]
    with patch(
        "udata_hydra.data_formats.table.to_parquet.db_to_parquet",
        new=_crash,
    ):
        await export_parquet(table=job_kw["table"], check=job_kw["check"])
    # we should still have the table and its reference in tables_index
    await db.execute(f'SELECT * FROM "{table_name}"')
    rows = list(
        await db.fetch("SELECT * FROM tables_index WHERE resource_id = $1", check["resource_id"])
    )
    assert len(rows) == 1
    assert rows[0]["parsing_table"] == table_name
    # yet we have the error where we should
    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    assert updated_check["parsing_error"] is not None
    assert updated_check["parquet_url"] is None


async def test_export_geojson_pmtiles_clears_status_on_failure(setup_catalog, fake_check, mocker):
    """When GeoJSON/PMTiles export fails, record the error and reset the resource status.
    Also removes leftover geojson/pmtiles files so a retry does not leave stale artifacts."""
    check = await fake_check()
    await Resource.set_job_status(RESOURCE_ID, "geojson", "CONVERTING_TO_GEOJSON")
    remove_remainders = mocker.patch("udata_hydra.analysis.exports.remove_remainders")
    mocker.patch("udata_hydra.analysis.exports.helpers.notify_udata")
    mocker.patch(
        "udata_hydra.analysis.exports.Table.to_geojson",
        side_effect=RuntimeError("export failed"),
    )

    await export_geojson_pmtiles(
        source=Table(table_name="test", resource_id=RESOURCE_ID),
        check=check,
    )

    remove_remainders.assert_called_once_with(RESOURCE_ID, ["geojson"])
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}
    updated_check = await Check.get_by_id(check["id"])
    assert updated_check is not None
    assert updated_check["parsing_error"] is not None


async def test_export_geojson_pmtiles_notifies_udata_on_success(setup_catalog, fake_check, mocker):
    """When GeoJSON/PMTiles export succeeds, notify udata and clear the resource status."""
    check = await fake_check()
    await Resource.set_job_status(RESOURCE_ID, "pmtiles", "CONVERTING_TO_PMTILES")
    notify_udata = mocker.patch(
        "udata_hydra.analysis.exports.helpers.notify_udata",
        new=mocker.AsyncMock(),
    )
    mocker.patch(
        "udata_hydra.analysis.exports.Table.to_geojson",
        new=mocker.AsyncMock(
            return_value=Geojson(path="tests/data/valid.geojson", resource_id=RESOURCE_ID)
        ),
    )
    mocker.patch(
        "udata_hydra.analysis.exports.Geojson.to_pmtiles",
        new=mocker.AsyncMock(
            return_value=PMTiles(
                path="tests/data/valid.geojson",
                resource_id=RESOURCE_ID,  # the actual file doesn't matter, we just need to be able to get its size on instanciation
            )
        ),
    )
    mocker.patch("udata_hydra.analysis.exports.context.s3_client", return_value=MagicMock())

    await export_geojson_pmtiles(
        source=Table(table_name="test", resource_id=RESOURCE_ID),
        check=check,
    )

    assert notify_udata.await_count == 2
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}


async def test_export_parquet_notifies_udata_on_success(setup_catalog, fake_check, mocker):
    """When parquet export succeeds, notify udata and clear the resource status."""
    check = await fake_check()
    await Resource.set_job_status(RESOURCE_ID, "parquet", "CONVERTING_TO_PARQUET")
    notify_udata = mocker.patch(
        "udata_hydra.analysis.exports.helpers.notify_udata",
        new=mocker.AsyncMock(),
    )
    mocker.patch(
        "udata_hydra.analysis.exports.Table.to_parquet",
        new=mocker.AsyncMock(
            return_value=Parquet(
                path="tests/data/valid.geojson",
                resource_id=RESOURCE_ID,  # the actual file doesn't matter, we just need to be able to get its size on instanciation
            )
        ),
    )
    mocker.patch("udata_hydra.analysis.exports.context.s3_client", return_value=MagicMock())

    await export_parquet(
        table=Table(table_name="test", resource_id=RESOURCE_ID),
        check=check,
    )

    notify_udata.assert_awaited_once()
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}


async def test_file_with_nan(
    setup_catalog,
    rmock,
    db,
    fake_check,
    produce_mock,
):
    check = await fake_check(headers={"content-type": "application/csv"})
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    rmock.get(
        url,
        status=200,
        headers={
            "content-type": "application/csv",
            "content-length": "100",
        },
        body=("a,b,c\n1,1.0,inf\n2,nan,2.0\n3,3.0,3.0\n").encode("utf-8"),
        repeat=True,
    )
    file = await download_from_check(check, Csv)
    await file.analyse(check=check)
    # it should all be fine
    rows = await db.fetch(f'SELECT * FROM "{table_name}"')
    assert dict(rows[0])["c"] == float("inf")
    assert dict(rows[1])["b"] is None
    profile = parse_json_value(
        list(
            await db.fetch(
                "SELECT * FROM tables_index WHERE resource_id = $1", check["resource_id"]
            )
        )[0]["csv_detective"]
    )["profile"]
    for col in ["a", "b"]:
        # NaN doesn't prevent the operations
        assert all(profile[col][method] is not None for method in ["min", "max", "mean", "std"])
    # inf does for max, mean and std
    assert all(profile["c"][method] is None for method in ["max", "mean", "std"])
    assert profile["c"]["min"] is not None

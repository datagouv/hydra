from datetime import datetime, timedelta
import hashlib
import json
import os
import pytest
import tempfile
from unittest.mock import MagicMock

import nest_asyncio

from aiohttp.client_exceptions import ClientError
from asyncio.exceptions import TimeoutError
from minicli import run
from yarl import URL

from udata_hydra import config
from udata_hydra.crawl import crawl
from udata_hydra.datalake_service import process_resource, compute_checksum_from_file


# TODO: make file content configurable
SIMPLE_CSV_CONTENT = """code_insee,number
95211,102
36522,48"""
resource_id = "c4e3a9fb-4415-488e-ba57-d05269b27adf"
dataset_id = "601ddcfc85a59c3a45c2435a"


pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def mock_download_resource(url, session, headers):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(SIMPLE_CSV_CONTENT.encode("utf-8"))
    tmp_file.close()
    return tmp_file


async def test_catalog(setup_catalog, db):
    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = $1",
        "c4e3a9fb-4415-488e-ba57-d05269b27adf",
    )
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == "https://example.com/resource-1"
    assert resource["dataset_id"] == "601ddcfc85a59c3a45c2435a"


async def test_catalog_deleted(setup_catalog, db, rmock):
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE")
    assert len(res) == 1
    with open("tests/catalog.csv", "rb") as cfile:
        catalog_content = cfile.readlines()
    catalog = "https://example.com/catalog"
    # feed empty catalog, should delete the previously loaded resource
    rmock.get(catalog, status=200, body=catalog_content[0])
    run("load_catalog", url=catalog)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_checked_resource(setup_catalog, db, rmock, event_loop, mocker, analysis_mock):
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    rurl = "https://example.com/resource-1"
    rmock.get(rurl)
    event_loop.run_until_complete(crawl(iterations=1))

    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE and last_check IS NOT NULL")
    assert len(res) == 1

    with open("tests/catalog.csv", "rb") as cfile:
        catalog_content = cfile.readlines()
    catalog = "https://example.com/catalog"
    # feed empty catalog, should delete the previously loaded resource
    rmock.get(catalog, status=200, body=catalog_content[0])
    run("load_catalog", url=catalog)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_new_url(setup_catalog, db, rmock, event_loop, mocker, analysis_mock):
    # load a new catalog with a new URL for this resource
    with open("tests/catalog.csv", "r") as cfile:
        catalog_content = cfile.readlines()
    catalog_content[-1] = catalog_content[-1].replace("resource-1", "resource-2")
    catalog_content = "\n".join(catalog_content)
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content.encode("utf-8"))
    run("load_catalog", url=catalog)

    # check catalog coherence
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog WHERE resource_id = $1", resource_id)
    assert len(res) == 2

    # check that the crawler does not crawl the deleted resource
    # check that udata is called only once
    # udata is not called for analysis results since it's mocked, only for checks
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    mocker.patch("udata_hydra.config.UDATA_URI_API_KEY", "my-api-key")
    rurl_1 = "https://example.com/resource-1"
    rurl_2 = "https://example.com/resource-2"
    rmock.get(rurl_1)
    rmock.get(rurl_2)
    rmock.put(udata_url, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl_1)) not in rmock.requests
    assert ("GET", URL(rurl_2)) in rmock.requests
    assert len(rmock.requests[("PUT", URL(udata_url))]) == 1


async def test_udata_connection_error_500(setup_catalog, mocker, analysis_mock, rmock, event_loop):
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    mocker.patch("udata_hydra.config.UDATA_URI_API_KEY", "my-api-key")
    rurl = "https://example.com/resource-1"
    rmock.get(rurl)
    rmock.put(udata_url, status=500)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl)) in rmock.requests
    assert ("PUT", URL(udata_url)) in rmock.requests


async def test_udata_connection_error_exception(setup_catalog, mocker, analysis_mock, rmock, event_loop):
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    mocker.patch("udata_hydra.config.UDATA_URI_API_KEY", "my-api-key")
    rurl = "https://example.com/resource-1"
    rmock.get(rurl)
    rmock.put(udata_url, exception=ClientError("client error"))
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl)) in rmock.requests
    assert ("PUT", URL(udata_url)) in rmock.requests


@pytest.mark.parametrize(
    "resource",
    [
        # status, timeout, exception
        (200, False, None),
        (500, False, None),
        (None, False, ClientError("client error")),
        (None, False, AssertionError),
        (None, False, UnicodeError),
        (None, True, TimeoutError),
    ],
)
async def test_crawl(setup_catalog, rmock, event_loop, db, resource, produce_mock, analysis_mock):
    status, timeout, exception = resource
    rurl = "https://example.com/resource-1"
    rmock.get(
        rurl,
        status=status,
        headers={"Content-LENGTH": "10", "X-Do": "you"},
        exception=exception,
    )
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl)) in rmock.requests
    res = await db.fetchrow("SELECT * FROM checks WHERE url = $1", rurl)
    assert res["url"] == rurl
    assert res["status"] == status
    if not exception:
        assert json.loads(res["headers"]) == {
            "x-do": "you",
            # added by aioresponses :shrug:
            "content-type": "application/json",
            "content-length": "10",
        }
    assert res["timeout"] == timeout
    if isinstance(exception, ClientError):
        assert res["error"] == "client error"
    elif status == 500:
        assert res["error"] == "Internal Server Error"
    else:
        assert not res["error"]


async def test_backoff(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock):
    await fake_check(resource=2)
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    magic = MagicMock()
    mocker.patch("udata_hydra.context.monitor").return_value = magic
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # verify that we actually backed-off
    assert magic.add_backoff.called


async def test_no_backoff_domains(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock
):
    await fake_check(resource=2)
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.NO_BACKOFF_DOMAINS", ["example.com"])
    magic = MagicMock()
    mocker.patch("udata_hydra.context.monitor").return_value = magic
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # verify that we actually did not back-off
    assert not magic.add_backoff.called


async def test_excluded_clause(setup_catalog, mocker, event_loop, rmock, produce_mock):
    mocker.patch("udata_hydra.config.SLEEP_BETWEEN_BATCHES", 0)
    mocker.patch("udata_hydra.config.EXCLUDED_PATTERNS", ["http%example%"])
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # url has not been called due to excluded clause
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_outdated_check(setup_catalog, rmock, fake_check, event_loop, produce_mock):
    await fake_check(created_at=datetime.now() - timedelta(weeks=52))
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # url has been called because check is outdated
    assert ("GET", URL(rurl)) in rmock.requests


async def test_not_outdated_check(
    setup_catalog, rmock, fake_check, event_loop, mocker, produce_mock
):
    mocker.patch("udata_hydra.config.SLEEP_BETWEEN_BATCHES", 0)
    await fake_check()
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # url has not been called because check is fresh
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_501_get(setup_catalog, event_loop, rmock, produce_mock):
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=501)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl)) in rmock.requests


async def test_process_resource(setup_catalog, mocker):
    rurl = "https://example.com/resource-1"

    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    # disable webhook, tested in following test
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    result = await process_resource(rurl, "dataset_id", resource_id, None, {})

    assert result["error"] is None
    assert result["checksum"] == hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest()
    assert result["filesize"] == len(SIMPLE_CSV_CONTENT)
    assert result["mime_type"] == "text/plain"


async def test_process_resource_send_udata(setup_catalog, mocker, rmock):
    rurl = "https://example.com/resource-1"
    udata_url = f"{config.UDATA_URI}/datasets/dataset_id/resources/{resource_id}/extras/"

    mocker.patch("udata_hydra.config.UDATA_URI_API_KEY", "my-api-key")
    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200)

    await process_resource(rurl, "dataset_id", resource_id, None, {})

    assert ("PUT", URL(udata_url)) in rmock.requests
    req = rmock.requests[("PUT", URL(udata_url))]
    assert len(req) == 1
    document = req[0].kwargs["json"]
    assert document["analysis:filesize"] == len(SIMPLE_CSV_CONTENT)
    assert document["analysis:mime"] == "text/plain"


async def test_compute_checksum_from_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(b"a very small file")
    tmp_file.close()

    checksum = await compute_checksum_from_file(tmp_file.name)
    assert checksum == hashlib.sha1(b"a very small file").hexdigest()
    os.remove(tmp_file.name)

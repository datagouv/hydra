from datetime import datetime, timedelta
import hashlib
import json
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
from udata_hydra.datalake_service import process_resource
from udata_hydra.utils.db import get_check


# TODO: make file content configurable
SIMPLE_CSV_CONTENT = """code_insee,number
95211,102
36522,48"""
resource_id = "c4e3a9fb-4415-488e-ba57-d05269b27adf"
dataset_id = "601ddcfc85a59c3a45c2435a"

pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def mock_download_resource(url, headers):
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
    rurl_1 = "https://example.com/resource-1"
    rurl_2 = "https://example.com/resource-2"
    rmock.head(rurl_1)
    rmock.head(rurl_2)
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    rmock.put(udata_url, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl_1)) not in rmock.requests
    assert ("HEAD", URL(rurl_2)) in rmock.requests
    assert len(rmock.requests[("PUT", URL(udata_url))]) == 1


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
    params = {
        "status": status,
        "headers": {"Content-LENGTH": "10", "X-Do": "you"},
        "exception": exception,
    }
    rmock.head(rurl, **params)
    # mock for head fallback
    rmock.get(rurl, **params)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
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
    await fake_check(resource=2, resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe")
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
    rmock.head(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # url has been called because check is outdated
    assert ("HEAD", URL(rurl)) in rmock.requests


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


async def test_switch_head_to_get(setup_catalog, event_loop, rmock, produce_mock):
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=501)
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) in rmock.requests


async def test_process_resource(setup_catalog, mocker, fake_check):
    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    # disable webhook, tested in following test
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    check = await fake_check()
    await process_resource(check["id"])
    result = await get_check(check["id"])

    assert result["error"] is None
    assert result["checksum"] == hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest()
    assert result["filesize"] == len(SIMPLE_CSV_CONTENT)
    assert result["mime_type"] == "text/plain"


async def test_process_resource_send_udata(setup_catalog, mocker, rmock, fake_check, db):
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"

    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200, repeat=True)

    check = await fake_check()
    await process_resource(check["id"])

    req = rmock.requests[("PUT", URL(udata_url))]
    assert len(req) == 1
    document = req[0].kwargs["json"]
    assert document["analysis:filesize"] == len(SIMPLE_CSV_CONTENT)
    assert document["analysis:mime-type"] == "text/plain"


async def test_process_resource_from_crawl(setup_catalog, rmock, event_loop, db):
    """"
    Looks a lot like an E2E test:
    - process catalog
    - check resource
    - download and analysis resource
    - trigger udata callbacks
    """

    rurl = "https://example.com/resource-1"

    # mock for check
    rmock.head(rurl, status=200, headers={"Content-Length": "200"})
    # mock for download
    rmock.get(rurl, status=200, body=SIMPLE_CSV_CONTENT.encode("utf-8"))
    # mock for check and analysis results
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    rmock.put(udata_url, status=200, repeat=True)

    event_loop.run_until_complete(crawl(iterations=1))

    assert len(rmock.requests[("PUT", URL(udata_url))]) == 2
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1
    assert res[0]["url"] == rurl
    assert res[0]["checksum"] is not None
    assert res[0]["status"] is not None


async def test_change_analysis_last_modified_header(setup_catalog, rmock, event_loop):
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    rmock.head("https://example.com/resource-1", headers={"last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2020-01-09T09:33:37"
    assert data["analysis:last-modified-detection"] == "last-modified-header"


async def test_change_analysis_content_length_header(setup_catalog, rmock, event_loop, fake_check, db):
    await fake_check(headers={"content-length": "1"})
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", resource_id)
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    rmock.head("https://example.com/resource-1", headers={"content-length": "2"})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.utcnow()
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "content-length-header"


async def test_change_analysis_checksum(setup_catalog, mocker, fake_check, db, rmock, event_loop):
    await fake_check(checksum="136bd31d53340d234957650e042172705bf32984")
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", resource_id)
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    rmock.head("https://example.com/resource-1", repeat=True)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.utcnow()
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "computed-checksum"


@pytest.mark.catalog_harvested
async def test_change_analysis_harvested(setup_catalog, mocker, rmock, event_loop):
    udata_url = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    mocker.patch("udata_hydra.datalake_service.download_resource", mock_download_resource)
    rmock.head("https://example.com/harvested", headers={"content-length": "2"}, repeat=True)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2022-12-06T05:00:32.647000"
    assert data["analysis:last-modified-detection"] == "harvest-resource-metadata"

import hashlib
import json
import pytest
import pytz
import sys
import tempfile

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import nest_asyncio

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError
from aioresponses import CallbackResult
from asyncio.exceptions import TimeoutError
from dateparser import parse as date_parser
from minicli import run
from yarl import URL

from udata_hydra import config
from udata_hydra.crawl import crawl, check_url, get_content_type_from_header, STATUS_BACKOFF
from udata_hydra.analysis.resource import process_resource
from udata_hydra.utils.db import get_check

from .conftest import RESOURCE_ID as resource_id
from .conftest import DATASET_ID as dataset_id


# TODO: make file content configurable
SIMPLE_CSV_CONTENT = """code_insee,number
95211,102
36522,48"""

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
    # Only one resource because the other belonged to an archived dataset
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == "https://example.com/resource-1"
    assert resource["dataset_id"] == "601ddcfc85a59c3a45c2435a"


async def test_catalog_deleted(setup_catalog, db, rmock):
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE")
    assert len(res) == 1
    with open("tests/data/catalog.csv", "rb") as cfile:
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

    with open("tests/data/catalog.csv", "rb") as cfile:
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
    with open("tests/data/catalog.csv", "r") as cfile:
        catalog_content = cfile.readlines()
    catalog_content[1] = catalog_content[1].replace("resource-1", "resource-2")
    catalog_content = "\n".join(catalog_content)
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content.encode("utf-8"))
    run("load_catalog", url=catalog)

    # check catalog coherence, replacing the URL in the existing entry
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", resource_id)
    assert len(res) == 1
    assert res[0]["deleted"] is False
    assert "resource-2" in res[0]["url"]


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
async def test_crawl(setup_catalog, rmock, event_loop, db, resource, analysis_mock, udata_url):
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
    rmock.put(udata_url)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests

    # test check results in DB
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

    # test webhook results from mock
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("check:date")
    datetime.fromisoformat(webhook["check:date"])
    if exception or status == 500:
        assert webhook.get("check:available") is False
    else:
        assert webhook.get("check:available")
        assert webhook.get("check:headers:content-type") == "application/json"
        assert webhook.get("check:headers:content-length") == 10
    if timeout:
        assert webhook.get("check:timeout")
    else:
        assert webhook.get("check:timeout") is False


async def test_backoff_nb_req(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock):
    await fake_check(resource=2, resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe")
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # verify that we actually backed-off
    assert ("HEAD", URL(rurl)) not in rmock.requests


@pytest.mark.parametrize(
    "ratelimit",
    [
        # remain, limit, should_backoff
        (0, 0, True),
        (1, 100, True),
        ("a", "b", False),
        (20, 100, False),
        (0, -1, False),
    ],
)
async def test_backoff_rate_limiting(setup_catalog, event_loop, rmock, fake_check, produce_mock, ratelimit):
    remain, limit, should_backoff = ratelimit
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": remain,
            "x-ratelimit-limit": limit,
        }
    )
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    # verify that we actually backed-off
    if should_backoff:
        assert ("HEAD", URL(rurl)) not in rmock.requests
    else:
        assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_rate_limiting_lifted(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db):
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": 1,  # our 10% quota has been reached
            "x-ratelimit-limit": 10,
        }
    )
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    # We should backoff
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_url(row, session)
    assert res == STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait for BACKOFF_PERIOD before crawling again, it should _not_ backoff
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res != STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_rate_limiting_cooled_off(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db):
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": 0,  # we've messed up
            "x-ratelimit-limit": 10,
        }
    )
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    mocker.patch("udata_hydra.config.COOL_OFF_PERIOD", 0.5)
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    # We should backoff
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_url(row, session)
    assert res == STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # waiting for BACKOFF_PERIOD is not enough since we've messed up already
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res == STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait until COOL_OFF_PERIOD (0.25+0.25) before crawling again,
    # it should _not_ backoff
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res != STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_nb_req_lifted(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db):
    await fake_check(resource=2, resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe")
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200)
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_url(row, session)
    assert res == STATUS_BACKOFF
    # verify that we actually backed-off
    assert ("HEAD", URL(rurl)) not in rmock.requests
    # we wait for BACKOFF_PERIOD before crawling again, it should _not_ backoff
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res != STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_on_429_status_code(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db):
    resource_id = "c5187912-24a5-49ea-a725-5e1e3d472efe"
    await fake_check(resource=2, resource_id=resource_id)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    mocker.patch("udata_hydra.config.COOL_OFF_PERIOD", 0.5)
    rurl = "https://example.com/resource-1"
    await db.execute("UPDATE checks SET status = 429 WHERE resource_id = $1", resource_id)
    rmock.head(rurl, status=200)
    row = await db.fetchrow("SELECT * FROM catalog")

    # we've messed up, we should backoff
    async with ClientSession() as session:
        res = await check_url(row, session)
    assert res == STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # waiting for BACKOFF_PERIOD is not enough since we've messed up already
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res == STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait until COOL_OFF_PERIOD (0.25+0.25) before crawling again,
    # it should _not_ backoff
    async with ClientSession() as session:
        res = await check_url(row, session, sleep=0.25)
    assert res != STATUS_BACKOFF
    assert ("HEAD", URL(rurl)) in rmock.requests


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


async def test_switch_head_to_get_headers(setup_catalog, event_loop, rmock, produce_mock):
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200, headers={})
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) in rmock.requests


async def test_no_switch_head_to_get(setup_catalog, event_loop, rmock, produce_mock, analysis_mock):
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200, headers={"content-length": "1"})
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_process_resource(setup_catalog, mocker, fake_check):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    # disable webhook, tested in following test
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    check = await fake_check()
    await process_resource(check["id"], False)
    result = await get_check(check["id"])

    assert result["error"] is None
    assert result["checksum"] == hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest()
    assert result["filesize"] == len(SIMPLE_CSV_CONTENT)
    assert result["mime_type"] == "text/plain"


async def test_process_resource_send_udata(setup_catalog, mocker, rmock, fake_check, udata_url):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200, repeat=True)

    check = await fake_check()
    await process_resource(check["id"], True)

    req = rmock.requests[("PUT", URL(udata_url))]
    assert len(req) == 1
    document = req[0].kwargs["json"]
    assert document["analysis:content-length"] == len(SIMPLE_CSV_CONTENT)
    assert document["analysis:mime-type"] == "text/plain"


async def test_process_resource_send_udata_no_change(setup_catalog, mocker, rmock, fake_check, udata_url):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200, repeat=True)

    # previous check with same checksum
    await fake_check(checksum=hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest())
    check = await fake_check()
    await process_resource(check["id"], False)

    # udata has not been called
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_process_resource_from_crawl(setup_catalog, rmock, event_loop, db, udata_url):
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
    rmock.put(udata_url, status=200, repeat=True)

    event_loop.run_until_complete(crawl(iterations=1))

    assert len(rmock.requests[("PUT", URL(udata_url))]) == 2
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1
    assert res[0]["url"] == rurl
    assert res[0]["checksum"] is not None
    assert res[0]["status"] is not None


async def test_change_analysis_last_modified_header(setup_catalog, rmock, event_loop, udata_url):
    rmock.head("https://example.com/resource-1", headers={"last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2020-01-09T09:33:37+00:00"
    assert data["analysis:last-modified-detection"] == "last-modified-header"


async def test_change_analysis_content_length_header(setup_catalog, rmock, event_loop, fake_check, db, udata_url):
    # different content-length than mock response
    await fake_check(headers={"content-length": "1"})
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", resource_id)
    rmock.head("https://example.com/resource-1", headers={"content-length": "2"})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.now(timezone.utc)
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "content-length-header"


async def test_change_analysis_checksum(setup_catalog, mocker, fake_check, db, rmock, event_loop, udata_url):
    # different checksum than mock file
    await fake_check(checksum="136bd31d53340d234957650e042172705bf32984")
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", resource_id)
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.head("https://example.com/resource-1")
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.now(pytz.UTC)
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "computed-checksum"


@pytest.mark.catalog_harvested
async def test_change_analysis_harvested(setup_catalog, mocker, rmock, event_loop, udata_url):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.head("https://example.com/harvested", headers={"content-length": "2"}, repeat=True)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2022-12-06T05:00:32.647000+00:00"
    assert data["analysis:last-modified-detection"] == "harvest-resource-metadata"


async def test_change_analysis_last_modified_header_twice(setup_catalog, rmock, event_loop, fake_check, udata_url):
    _date = "Thu, 09 Jan 2020 09:33:37 GMT"
    await fake_check(detected_last_modified_at=date_parser(_date), created_at=datetime.now() - timedelta(days=10))
    rmock.head("https://example.com/resource-1", headers={"last-modified": _date})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    # udata has not been called: not first check, outdated check, and last-modified stayed the same
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_change_analysis_last_modified_header_twice_tz(setup_catalog, rmock, event_loop, fake_check, udata_url):
    _date_1 = "Thu, 09 Jan 2020 09:33:37 GMT+1"
    _date_2 = "Thu, 09 Jan 2020 09:33:37 GMT+4"
    await fake_check(detected_last_modified_at=date_parser(_date_1), created_at=datetime.now() - timedelta(days=10))
    rmock.head("https://example.com/resource-1", headers={"last-modified": _date_2})
    rmock.get("https://example.com/resource-1")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(crawl(iterations=1))
    # udata has been called: last-modified has changed (different timezones)
    assert ("PUT", URL(udata_url)) in rmock.requests


async def test_crawl_and_analysis_user_agent(setup_catalog, rmock, event_loop, produce_mock):
    # very complicated stuff, thanks https://github.com/pnuckowski/aioresponses/issues/111#issuecomment-896585061
    def callback(url, **kwargs):
        assert config.USER_AGENT == sys._getframe(3).f_locals["orig_self"].headers["user-agent"]
        # add content-length to avoid switching from HEAD to GET when crawling
        return CallbackResult(status=200, payload={}, headers={"content-length": "1"})
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, callback=callback)
    rmock.get(rurl, callback=callback)
    event_loop.run_until_complete(crawl(iterations=1))


async def test_crawl_triggered_by_udata_entrypoint_clean_catalog(
    client, udata_resource_payload, event_loop, db, rmock, analysis_mock, clean_db, produce_mock,
):
    rurl = udata_resource_payload["document"]["url"]
    rmock.head(rurl, headers={"content-length": "1"})
    res = await client.post("/api/resource/created/", json=udata_resource_payload)
    assert res.status == 200
    res = await db.fetch("SELECT * FROM catalog")
    assert len(res) == 1
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1


async def test_crawl_triggered_by_udata_entrypoint_existing_catalog(
    setup_catalog, client, udata_resource_payload, event_loop, db, rmock, analysis_mock, produce_mock,
):
    rurl = udata_resource_payload["document"]["url"]
    rmock.head(rurl, headers={"content-length": "1"})
    res = await client.post("/api/resource/created/", json=udata_resource_payload)
    assert res.status == 200
    res = await db.fetch("SELECT * FROM catalog")
    assert len(res) == 2
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 2


async def test_crawl_triggers_csv_analysis(rmock, event_loop, db, produce_mock, setup_catalog):
    """Crawl a CSV file, analyse and apify it, downloads only once"""
    rurl = "https://example.com/resource-1"
    # mock for check
    rmock.head(rurl, status=200, headers={"content-length": "1", "content-type": "application/csv"})
    # mock for analysis download
    rmock.get(rurl, status=200, headers={"content-type": "application/csv"}, body=SIMPLE_CSV_CONTENT.encode("utf-8"))
    event_loop.run_until_complete(crawl(iterations=1))
    # GET called only once: HEAD is ok (no need for crawl) and analysis steps share the downloaded file
    assert len(rmock.requests[("GET", URL(rurl))]) == 1
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1
    assert res[0]["parsing_table"] is not None
    res = await db.fetch(f'SELECT * FROM "{res[0]["parsing_table"]}"')
    assert len(res) == 2


async def test_recrawl_download_only_once(rmock, fake_check, event_loop, db, produce_mock, setup_catalog):
    """On recrawl of a (CSV) file, if it hasn't change, downloads only once"""
    await fake_check(
        resource_id=resource_id,
        headers={
            "last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"
        }
    )
    rurl = "https://example.com/resource-1"
    # mock for check, with same last-modified header
    rmock.head(rurl, status=200, headers={
        "last-modified": "Thu, 09 Jan 2020 09:33:37 GMT",
        "content-type": "application/csv"
    })
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", resource_id)
    event_loop.run_until_complete(crawl(iterations=1))

    # HEAD should have been called
    assert len(rmock.requests[("HEAD", URL(rurl))]) == 1

    # GET shouldn't have been called
    assert ("GET", URL(rurl)) not in rmock.requests


@pytest.mark.parametrize(
    "content_type",
    [
        # (content type header, parsed content type)
        ("application/json", "application/json"),
        ("text/html; charset=utf-8", "text/html"),
        ("text/html;h5ai=0.20;charset=UTF-8", "text/html")
    ],
)
async def test_content_type_from_header(content_type):
    content_type_header, parsed_content_type = content_type
    assert parsed_content_type == await get_content_type_from_header({"content-type": content_type_header})


async def test_dont_crawl_urls_with_status_crawling(rmock, event_loop, db, produce_mock, setup_catalog):
    """Don't crawl urls that have a status state pending"""
    rurl = "https://example.com/resource-1"
    await db.execute("UPDATE catalog SET priority = TRUE, status = 'crawling' WHERE resource_id = $1", resource_id)
    event_loop.run_until_complete(crawl(iterations=1))

    # HEAD shouldn't have been called
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # GET shouldn't have been called
    assert ("GET", URL(rurl)) not in rmock.requests

from datetime import datetime, timedelta
import json
from unittest.mock import MagicMock
import pytest

import nest_asyncio

from aiohttp.client_exceptions import ClientError
from asyncio.exceptions import TimeoutError
from yarl import URL

from udata_hydra.crawl import crawl, setup_logging


pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def test_catalog(setup_catalog, db):
    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = $1",
        "c4e3a9fb-4415-488e-ba57-d05269b27adf",
    )
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == "https://example.com/resource-1"
    assert resource["dataset_id"] == "601ddcfc85a59c3a45c2435a"


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
async def test_crawl(setup_catalog, rmock, event_loop, db, resource, mocker, produce_mock):
    setup_logging()
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
    setup_logging()
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
    setup_logging()
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
    setup_logging()
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
    setup_logging()
    rurl = "https://example.com/resource-1"
    rmock.get(rurl, status=501)
    event_loop.run_until_complete(crawl(iterations=1))
    assert ("GET", URL(rurl)) in rmock.requests

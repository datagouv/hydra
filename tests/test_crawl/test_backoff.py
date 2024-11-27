from unittest.mock import MagicMock

import nest_asyncio
import pytest
from aiohttp import ClientSession
from yarl import URL

from tests.conftest import RESOURCE_URL
from udata_hydra.crawl.check_resources import (
    RESOURCE_RESPONSE_STATUSES,
    check_resource,
)
from udata_hydra.crawl.start_checks import start_checks

# TODO: make file content configurable
SIMPLE_CSV_CONTENT = """code_insee,number
95211,102
36522,48"""

pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def test_backoff_nb_req(setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock):
    await fake_check(resource=2, resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe")
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
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
async def test_backoff_rate_limiting(
    setup_catalog, event_loop, rmock, fake_check, produce_mock, ratelimit
):
    remain, limit, should_backoff = ratelimit
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": remain,
            "x-ratelimit-limit": limit,
        },
    )
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    # verify that we actually backed-off
    if should_backoff:
        assert ("HEAD", URL(rurl)) not in rmock.requests
    else:
        assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_rate_limiting_lifted(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db
):
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": 1,  # our 10% quota has been reached
            "x-ratelimit-limit": 10,
        },
    )
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    # We should backoff
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_resource(url=row["url"], resource_id=row["resource_id"], session=session)
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait for BACKOFF_PERIOD before crawling again, it should _not_ backoff
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res != RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_rate_limiting_cooled_off(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db
):
    await fake_check(
        resource=2,
        resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe",
        headers={
            "x-ratelimit-remaining": 0,  # we've messed up
            "x-ratelimit-limit": 10,
        },
    )
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    mocker.patch("udata_hydra.config.COOL_OFF_PERIOD", 0.5)
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    rmock.get(rurl, status=200)
    # We should backoff
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_resource(url=row["url"], resource_id=row["resource_id"], session=session)
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # waiting for BACKOFF_PERIOD is not enough since we've messed up already
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait until COOL_OFF_PERIOD (0.25+0.25) before crawling again,
    # it should _not_ backoff
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res != RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_nb_req_lifted(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db
):
    await fake_check(resource=2, resource_id="c5187912-24a5-49ea-a725-5e1e3d472efe")
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    row = await db.fetchrow("SELECT * FROM catalog")
    async with ClientSession() as session:
        res = await check_resource(url=row["url"], resource_id=row["resource_id"], session=session)
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    # verify that we actually backed-off
    assert ("HEAD", URL(rurl)) not in rmock.requests
    # we wait for BACKOFF_PERIOD before crawling again, it should _not_ backoff
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res != RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_backoff_on_429_status_code(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock, db
):
    resource_id = "c5187912-24a5-49ea-a725-5e1e3d472efe"
    await fake_check(resource=2, resource_id=resource_id)
    mocker.patch("udata_hydra.config.BACKOFF_PERIOD", 0.25)
    mocker.patch("udata_hydra.config.COOL_OFF_PERIOD", 0.5)
    rurl = RESOURCE_URL
    await db.execute("UPDATE checks SET status = 429 WHERE resource_id = $1", resource_id)
    rmock.head(rurl, status=200)
    row = await db.fetchrow("SELECT * FROM catalog")

    # we've messed up, we should backoff
    async with ClientSession() as session:
        res = await check_resource(url=row["url"], resource_id=row["resource_id"], session=session)
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # waiting for BACKOFF_PERIOD is not enough since we've messed up already
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res == RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) not in rmock.requests

    # we wait until COOL_OFF_PERIOD (0.25+0.25) before crawling again,
    # it should _not_ backoff
    async with ClientSession() as session:
        res = await check_resource(
            url=row["url"], resource_id=row["resource_id"], session=session, sleep=0.25
        )
    assert res != RESOURCE_RESPONSE_STATUSES["BACKOFF"]
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_no_backoff_domains(
    setup_catalog, event_loop, rmock, mocker, fake_check, produce_mock
):
    await fake_check(resource=2)
    mocker.patch("udata_hydra.config.BACKOFF_NB_REQ", 1)
    mocker.patch("udata_hydra.config.NO_BACKOFF_DOMAINS", ["example.com"])
    magic = MagicMock()
    mocker.patch("udata_hydra.context.monitor").return_value = magic
    rurl = RESOURCE_URL
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    # verify that we actually did not back-off
    assert not magic.add_backoff.called

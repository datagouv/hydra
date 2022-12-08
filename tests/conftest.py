import asyncio
import os

from unittest import mock

from aioresponses import aioresponses
import asyncpg
import pytest
import pytest_asyncio

from minicli import run

import udata_hydra.cli  # noqa - this register the cli cmds
from udata_hydra.utils.db import insert_check, update_check

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
pytestmark = pytest.mark.asyncio


def dummy(return_value=None):
    """
    Creates a generic function which returns what is asked
    A kind of MagicMock but pickle-able for workers
    You should use this when mocking an enqueued function
    """
    async def fn(*args, **kwargs):
        return return_value
    return fn


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "catalog_harvested: use catalog_harvested.csv as source"
    )


@pytest.fixture
def is_harvested(request):
    return "catalog_harvested" in [m.name for m in request.node.iter_markers()]


# this really really really should run first (or "prod" db will get erased)
@pytest.fixture(autouse=True, scope="session")
def setup():
    with mock.patch.multiple(
        "udata_hydra.config",
        DATABASE_URL=DATABASE_URL,
        UDATA_URI="https://udata.example.com",
        UDATA_URI_API_KEY="sup3rs3cr3t",
        TESTING=True,
        SLEEP_BETWEEN_BATCHES=0,
    ):
        yield


@pytest_asyncio.fixture(autouse=True)
async def mock_pool(mocker, event_loop):
    """This avoids having different pools attached to different event loops"""
    m = mocker.patch("udata_hydra.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50, loop=event_loop)
    m.return_value = pool


@pytest_asyncio.fixture(autouse=True)
async def patch_enqueue(mocker, event_loop):
    """
    Patch our enqueue helper
    This bypasses rq totally by executing the function in the same event loop
    This also has the advantage of bubbling up errors in queued functions
    """
    def _execute(fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            loop = event_loop
            coro_result = loop.run_until_complete(result)
            return coro_result
        return result
    mocker.patch("udata_hydra.utils.queue.enqueue", _execute)


@pytest.fixture
def catalog_content(is_harvested):
    filename = "catalog" if not is_harvested else "catalog_harvested"
    with open(f"tests/{filename}.csv", "rb") as cfile:
        return cfile.read()


@pytest.fixture
def setup_catalog(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("drop_db")
    run("migrate")
    run("load_catalog", url=catalog)


@pytest.fixture
def produce_mock(mocker):
    mocker.patch("udata_hydra.crawl.send", dummy())
    mocker.patch("udata_hydra.datalake_service.send", dummy())


@pytest.fixture
def analysis_mock(mocker):
    """Disable process_resource while crawling"""
    mocker.patch("udata_hydra.crawl.process_resource", dummy({
        "error": None,
        "checksum": None,
        "filesize": None,
        "mime_type": None
    }))


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def db():
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def fake_check(db):
    async def _fake_check(
        status=200,
        error=None,
        timeout=False,
        resource=1,
        created_at=None,
        headers={"x-do": "you"},
        checksum=None,
        resource_id="c4e3a9fb-4415-488e-ba57-d05269b27adf",
    ):
        data = {
            "url": f"https://example.com/resource-{resource}",
            "domain": "example.com",
            "status": status,
            "headers": headers,
            "timeout": timeout,
            "response_time": 0.1,
            "resource_id": resource_id,
            "error": error,
            "checksum": checksum,
        }
        id = await insert_check(data)
        data["id"] = id
        if created_at:
            await update_check(id, {"created_at": created_at})
            data["created_at"] = created_at
        return data

    return _fake_check

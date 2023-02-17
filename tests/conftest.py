import asyncio
import hashlib
import os

from datetime import datetime

import asyncpg
import nest_asyncio
import pytest
import pytest_asyncio

from aioresponses import aioresponses
from aiohttp.test_utils import TestClient, TestServer
from minicli import run

from udata_hydra import config
from udata_hydra.app import app_factory
import udata_hydra.cli  # noqa - this register the cli cmds
from udata_hydra.logger import stop_sentry
from udata_hydra.db import checks

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
RESOURCE_ID = "c4e3a9fb-4415-488e-ba57-d05269b27adf"
DATASET_ID = "601ddcfc85a59c3a45c2435a"
pytestmark = pytest.mark.asyncio

nest_asyncio.apply()


def dummy(return_value=None):
    """
    Creates a generic function which returns what is asked
    A kind of MagicMock but pickle-able for workers
    You should use this when mocking an enqueued function
    """
    async def fn(*args, **kwargs):
        return return_value
    return fn


@pytest.fixture
def is_harvested(request):
    return "catalog_harvested" in [m.name for m in request.node.iter_markers()]


# this really really really should run first (or "prod" db will get erased)
@pytest.fixture(autouse=True, scope="session")
def setup():
    config.override(
        DATABASE_URL=DATABASE_URL,
        # use same database for tests
        DATABASE_URL_CSV=DATABASE_URL,
        UDATA_URI="https://udata.example.com",
        UDATA_URI_API_KEY="sup3rs3cr3t",
        TESTING=True,
        SLEEP_BETWEEN_BATCHES=0,
        WEBHOOK_ENABLED=True,
        SENTRY_DSN=None,
    )
    # prevent sentry from sending events in tests (config override is not enough)
    stop_sentry()


@pytest_asyncio.fixture(autouse=True)
async def mock_pool(mocker, event_loop):
    """This avoids having different pools attached to different event loops"""
    m = mocker.patch("udata_hydra.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50, loop=event_loop)
    m.return_value = pool
    yield
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def patch_enqueue(mocker, event_loop):
    """
    Patch our enqueue helper
    This bypasses rq totally by executing the function in the same event loop
    This also has the advantage of bubbling up errors in queued functions
    """
    def _execute(fn, *args, **kwargs):
        kwargs.pop("_priority")
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            loop = event_loop
            coro_result = loop.run_until_complete(result)
            return coro_result
        return result
    mocker.patch("udata_hydra.utils.queue.enqueue", _execute)


@pytest_asyncio.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture
def catalog_content(is_harvested):
    filename = "catalog" if not is_harvested else "catalog_harvested"
    with open(f"tests/data/{filename}.csv", "rb") as cfile:
        return cfile.read()


@pytest.fixture
def clean_db():
    run("drop_dbs", dbs=["main"])
    run("migrate")
    yield


@pytest.fixture
def setup_catalog(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("drop_dbs", dbs=["main"])
    run("migrate")
    run("load_catalog", url=catalog)


@pytest.fixture
def produce_mock(mocker):
    mocker.patch("udata_hydra.crawl.send", dummy())
    mocker.patch("udata_hydra.analysis.resource.send", dummy())
    mocker.patch("udata_hydra.analysis.csv.send", dummy())


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
async def fake_check():
    async def _fake_check(
        status=200,
        error=None,
        timeout=False,
        resource=1,
        created_at=None,
        headers={"x-do": "you"},
        checksum=None,
        resource_id="c4e3a9fb-4415-488e-ba57-d05269b27adf",
        detected_last_modified_at=None,
        parsing_table=False,
    ):
        url = f"https://example.com/resource-{resource}"
        data = {
            "url": url,
            "domain": "example.com",
            "status": status,
            "headers": headers,
            "timeout": timeout,
            "response_time": 0.1,
            "resource_id": resource_id,
            "error": error,
            "checksum": checksum,
            "detected_last_modified_at": detected_last_modified_at,
            "parsing_table": hashlib.md5(url.encode("utf-8")).hexdigest() if parsing_table else None,
        }
        id = await checks.insert(data)
        data["id"] = id
        if created_at:
            await checks.update(id, {"created_at": created_at})
            data["created_at"] = created_at
        return data

    return _fake_check


@pytest.fixture
def udata_url():
    return f"{config.UDATA_URI}/datasets/{DATASET_ID}/resources/{RESOURCE_ID}/extras/"


@pytest.fixture
def udata_resource_payload():
    return {
        "resource_id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
        "dataset_id": "61fd30cb29ea95c7bc0e1211",
        "document": {
            "id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
            "url": "http://dev.local/",
            "title": "random title",
            "description": "random description",
            "filetype": "file",
            "type": "documentation",
            "mime": "text/plain",
            "filesize": 1024,
            "checksum_type": "sha1",
            "checksum_value": "b7b1cd8230881b18b6b487d550039949867ec7c5",
            "created_at": datetime.now().isoformat(),
            "modified": datetime.now().isoformat(),
            "published": datetime.now().isoformat(),
        }
    }

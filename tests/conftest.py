import asyncio
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime

import asyncpg
import nest_asyncio
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

import udata_hydra.cli  # noqa - this register the cli cmds
from udata_hydra import config
from udata_hydra.app import app_factory
from udata_hydra.cli import drop_dbs, load_catalog, migrate
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.logger import stop_sentry

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
RESOURCE_ID = "c4e3a9fb-4415-488e-ba57-d05269b27adf"
RESOURCE_EXCEPTION_ID = "d4e3a9fb-4415-488e-ba57-d05269b27adf"
RESOURCE_EXCEPTION_TABLE_INDEXES = {"Nom": "index", "N° de certificat": "index"}
RESOURCE_URL = "https://example.com/resource-1"
DATASET_ID = "601ddcfc85a59c3a45c2435a"
NOT_EXISTING_RESOURCE_ID = "5d0b2b91-b21b-4120-83ef-83f818ba2451"

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()

log = logging.getLogger("udata-hydra")


def dummy(return_value=None):
    """
    Creates a generic function which returns what is asked
    A kind of MagicMock but pickle-able for workers
    You should use this when mocking an enqueued function
    """

    async def fn(*args, **kwargs):
        return return_value

    return fn


def pytest_addoption(parser):
    parser.addoption(
        "--input_file",
        action="store",
        default=None,
        help="Path to input file for performance tests",
    )


def pytest_generate_tests(metafunc):
    # This is called for every test. Check if the test function has the parameters
    # we want to parametrize and if the command line options are provided.
    input_file_value = metafunc.config.option.input_file

    # Check if the test function has 'input_file' as a parameter
    if (
        hasattr(metafunc.function, "__code__")
        and "input_file" in metafunc.function.__code__.co_varnames
    ):
        metafunc.parametrize("input_file", [input_file_value])


@pytest.fixture
def is_harvested(request):
    return "catalog_harvested" in [m.name for m in request.node.iter_markers()]


@pytest.fixture
def api_headers() -> dict:
    return {"Authorization": f"Bearer {config.API_KEY}"}


@pytest.fixture
def api_headers_wrong_token() -> dict:
    return {"Authorization": "Bearer stupid-token"}


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
async def mock_pool(mocker):
    """This avoids having different pools attached to different event loops"""
    m = mocker.patch("udata_hydra.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50)
    m.return_value = pool
    yield
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def patch_enqueue(mocker):
    """
    Patch our enqueue helper
    This bypasses rq totally by executing the function in the same event loop
    This also has the advantage of bubbling up errors in queued functions
    """

    def _execute(fn, *args, **kwargs):
        kwargs.pop("_priority")
        kwargs.pop("_exception", None)
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            loop = asyncio.get_running_loop()
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


async def _cleanup_cli_connections():
    """Helper function to clean up CLI connection context between tests"""
    from udata_hydra.cli import context

    for conn in list(context["conn"].values()):
        if not conn.is_closed():
            try:
                await conn.close()
            except (RuntimeError, AttributeError):
                # Connection might be attached to a different/closed event loop
                # Just mark it as closed in our context
                pass
    context["conn"].clear()


@pytest_asyncio.fixture
async def clean_db():
    await _cleanup_cli_connections()
    await drop_dbs(dbs=["main"])
    await migrate(skip_errors=False, dbs=["main", "csv"])
    yield
    await _cleanup_cli_connections()


@pytest_asyncio.fixture
async def setup_catalog(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    await _cleanup_cli_connections()
    await drop_dbs(dbs=["main"])
    await migrate(skip_errors=False, dbs=["main", "csv"])
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)
    yield
    await _cleanup_cli_connections()


@pytest.fixture
async def setup_catalog_with_resource_exception(setup_catalog):
    """Setup a catalog with a resource that is too large to be processed
    Columns for the resource RESOURCE_ID_EXCEPTION:
    ['__id', 'Nom', 'Prenom', 'Societe', 'Adresse', 'CP', 'Ville', 'Tel1', 'Tel2', 'email', 'Organisme', 'Org Cofrac', 'Type de certificat', 'N° de certificat', 'Date début validité', 'Date fin validité']
    """
    await Resource.insert(
        dataset_id=DATASET_ID,
        resource_id=RESOURCE_EXCEPTION_ID,
        type="main",
        format="csv",
        url="http://example.com/",
    )
    await ResourceException.insert(
        resource_id=RESOURCE_EXCEPTION_ID,
        table_indexes=RESOURCE_EXCEPTION_TABLE_INDEXES,
        comment="This is a test comment.",
    )


@pytest.fixture
def produce_mock(mocker):
    mocker.patch("udata_hydra.crawl.preprocess_check_data.send", dummy())
    mocker.patch("udata_hydra.analysis.resource.send", dummy())
    mocker.patch("udata_hydra.analysis.helpers.send", dummy())


@pytest.fixture
def analysis_mock(mocker):
    """Disable analyse_resource while crawling"""
    mocker.patch(
        "udata_hydra.analysis.resource.analyse_resource",
        dummy({"error": None, "checksum": None, "filesize": None, "mime_type": None}),
    )


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
async def insert_fake_resource():
    async def _insert_fake_resource(database, status: str | None = None):
        await Resource.insert(
            dataset_id=DATASET_ID,
            resource_id=RESOURCE_ID,
            url=RESOURCE_URL,
            type="main",
            format="csv",
            status=status,
            priority=True,
        )

    return _insert_fake_resource


@pytest_asyncio.fixture
def fake_resource_id():
    return uuid.uuid4


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
        mime_type=None,
        filesize=None,
        analysis_error=None,
        resource_id=RESOURCE_ID,
        detected_last_modified_at=None,
        next_check_at=None,
        parsing_table=False,
        parquet_url=False,
        domain="example.com",
        pmtiles_url=False,
        geojson_url=False,
        url=None,
    ) -> dict:
        _url = url or f"https://example.com/resource-{resource}"
        data = {
            "url": _url,
            "domain": domain,
            "status": status,
            "headers": json.dumps(headers),
            "timeout": timeout,
            "response_time": 0.1,
            "resource_id": resource_id,
            "error": error,
            "checksum": checksum,
            "mime_type": mime_type,
            "filesize": filesize,
            "analysis_error": analysis_error,
            "detected_last_modified_at": detected_last_modified_at,
            "next_check_at": next_check_at,
            "parsing_table": hashlib.md5(_url.encode("utf-8")).hexdigest()
            if parsing_table
            else None,
            "parquet_url": "https://example.org/file.parquet" if parquet_url else None,
            "parquet_size": 2048 if parquet_url else None,
            "pmtiles_url": "https://example.org/file.pmtiles" if pmtiles_url else None,
            "pmtiles_size": 1024 if pmtiles_url else None,
            "geojson_url": "https://example.org/file.geojson" if pmtiles_url else None,
            "geojson_size": 1024 if geojson_url else None,
        }
        check: dict = await Check.insert(data=data, returning="*")
        data["id"] = check["id"]
        if check.get("dataset_id"):
            data["dataset_id"] = check["dataset_id"]
        if created_at:
            await Check.update(check["id"], {"created_at": created_at})
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
            "format": "pdf",
            "mime": "text/plain",
            "schema": None,
            "filesize": 1024,
            "checksum_type": "sha1",
            "checksum_value": "b7b1cd8230881b18b6b487d550039949867ec7c5",
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
        },
    }

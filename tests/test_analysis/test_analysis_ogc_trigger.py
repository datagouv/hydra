"""When analyse_resource enqueues the OGC analysis, independently of the change detection"""

import hashlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from udata_hydra import config
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.data_formats import Wfs
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio

WFS_URL = "https://example.com/geoserver/wfs?SERVICE=WFS&REQUEST=GetCapabilities"
CAPABILITIES = b"<wfs:WFS_Capabilities>same document</wfs:WFS_Capabilities>"
CAPABILITIES_CHECKSUM = hashlib.sha1(CAPABILITIES).hexdigest()
TOO_LARGE = str(Wfs.max_filesize_allowed + 1)


@pytest.fixture
def ogc_analyse(mocker):
    mocker.patch("udata_hydra.config.OGC_ANALYSIS_ENABLED", True)
    mocker.patch("udata_hydra.config.OGC_FORMATS", ["wfs", "wms"])
    return mocker.patch.object(Wfs, "analyse", new=AsyncMock())


@pytest.fixture
def previous_check(fake_check):
    """A check already analysed (it has a checksum), optionally with an OGC analysis attempt"""

    async def _previous_check(ogc_analysis_age: timedelta | None = None, **kwargs) -> dict:
        kwargs = {
            "url": WFS_URL,
            "checksum": CAPABILITIES_CHECKSUM,
            "filesize": len(CAPABILITIES),
            "created_at": datetime.now(timezone.utc) - timedelta(days=60),
        } | kwargs
        check = await fake_check(**kwargs)
        if ogc_analysis_age is not None:
            await Check.update(
                check["id"],
                {"parsing_started_at": datetime.now(timezone.utc) - ogc_analysis_age},
            )
        return check

    return _previous_check


async def _analyse_new_check(fake_check, rmock, force_analysis=False, **kwargs) -> dict:
    check = await fake_check(url=WFS_URL, **kwargs)
    rmock.get(WFS_URL, status=200, body=CAPABILITIES)
    await analyse_resource(check=check, force_analysis=force_analysis)
    return check


async def _assert_status_reset(check: dict) -> None:
    resource = await Resource.get(check["resource_id"])
    assert resource is not None
    assert resource["status"] is None


async def test_first_check_is_analysed(setup_catalog, rmock, fake_check, produce_mock, ogc_analyse):
    await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_called_once()


async def test_unchanged_and_never_analysed(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    """e.g. geo-ide: the GetCapabilities never changes, so the analysis used to never run"""
    await previous_check()
    await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_called_once()


async def test_unchanged_and_recently_analysed(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    await previous_check(ogc_analysis_age=timedelta(days=1))
    check = await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_not_called()
    await _assert_status_reset(check)


async def test_unchanged_and_analysis_too_old(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    await previous_check(ogc_analysis_age=timedelta(days=config.OGC_ANALYSIS_MAX_AGE_DAYS + 1))
    await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_called_once()


async def test_changed_and_recently_analysed(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    """e.g. GeoServer: the GetCapabilities changes on every check, no need to re-analyse each time"""
    await previous_check(ogc_analysis_age=timedelta(days=1), checksum="another-checksum")
    check = await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_not_called()
    await _assert_status_reset(check)


async def test_forced_analysis(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    await previous_check(ogc_analysis_age=timedelta(days=1))
    await _analyse_new_check(fake_check, rmock, force_analysis=True)
    ogc_analyse.assert_called_once()


async def test_previous_analysis_on_another_url(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    await previous_check(ogc_analysis_age=timedelta(days=1), url="https://example.com/old/wfs")
    await _analyse_new_check(fake_check, rmock)
    ogc_analyse.assert_called_once()


async def test_too_large_on_download(setup_catalog, rmock, fake_check, produce_mock, ogc_analyse):
    check = await _analyse_new_check(fake_check, rmock, headers={"content-length": TOO_LARGE})
    ogc_analyse.assert_not_called()
    await _assert_status_reset(check)


async def test_too_large_known_from_previous_check(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    """No download when the headers say nothing changed: rely on the last known filesize"""
    headers = {"content-length": "0"}
    await previous_check(
        headers=headers,
        filesize=Wfs.max_filesize_allowed + 1,
        detected_last_modified_at=datetime.now(timezone.utc) - timedelta(days=60),
    )
    check = await _analyse_new_check(fake_check, rmock, headers=headers)
    ogc_analyse.assert_not_called()
    await _assert_status_reset(check)


async def test_unchanged_from_headers_and_never_analysed(
    setup_catalog, rmock, fake_check, previous_check, produce_mock, ogc_analyse
):
    """e.g. GeoBretagne since 2026-10-20: constant content-length, so no download at all"""
    headers = {"content-length": "0"}
    await previous_check(
        headers=headers,
        detected_last_modified_at=datetime.now(timezone.utc) - timedelta(days=60),
    )
    await _analyse_new_check(fake_check, rmock, headers=headers)
    ogc_analyse.assert_called_once()

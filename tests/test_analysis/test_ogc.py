import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from owslib.util import ServiceException
from requests.exceptions import ConnectionError as RequestsConnectionError

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.ogc import analyse_ogc
from udata_hydra.utils.ogc import detect_ogc

log = logging.getLogger("udata-hydra")


class TestOgcDetection:
    """Tests for OGC service detection"""

    @pytest.mark.parametrize("url,expected", [
        ("https://example.com/geoserver?SERVICE=WFS&REQUEST=GetCapabilities", True),
        ("https://example.com/geoserver?service=wfs&request=GetCapabilities", True),
        ("https://example.com/geoserver?Service=Wfs&Request=GetCapabilities", True),
        ("https://example.com/geoserver/wfs", True),
        ("https://example.com/geoserver/WFS", True),
        ("https://example.com/geoserver/wfs?request=GetCapabilities", True),
        ("https://example.com/data/wfsx", False),
        ("https://example.com/data/file.csv", False),
        ("https://example.com/geoserver?SERVICE=WMS&REQUEST=GetCapabilities", False),
        ("", False),
    ])
    def test_detect_wfs_from_url(self, url, expected):
        check = {"url": url}
        assert detect_ogc(check) is expected

    def test_detect_missing_url(self):
        check = {}
        assert detect_ogc(check) is False

    @pytest.mark.parametrize("resource_format,expected", [
        ("wfs", True),
        ("WFS", True),
        ("ogc:wfs", True),
        ("OGC:WFS", True),
        ("wms", False),
        ("csv", False),
        (None, False),
    ])
    def test_detect_wfs_from_format(self, resource_format, expected):
        check = {"url": "https://example.com/data"}
        assert detect_ogc(check, resource_format) is expected


@pytest.mark.asyncio
class TestOgcAnalysis:
    """Tests for OGC analysis"""

    async def test_analyse_wfs_disabled(self, setup_catalog, fake_check):
        with patch("udata_hydra.analysis.ogc.config") as mock_config:
            mock_config.OGC_ANALYSIS_ENABLED = False
            check = await fake_check()
            result = await analyse_ogc(check)
            assert result is None

    async def test_analyse_wfs_success(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_crs_4326 = MagicMock()
        mock_crs_4326.getcode.return_value = "EPSG:4326"
        mock_crs_3857 = MagicMock()
        mock_crs_3857.getcode.return_value = "EPSG:3857"

        mock_layer = MagicMock()
        mock_layer.crsOptions = [mock_crs_4326, mock_crs_3857]

        mock_get_feature = MagicMock()
        mock_get_feature.parameters = {
            "outputFormat": {"values": ["application/json", "text/xml"]}
        }

        mock_wfs = MagicMock()
        mock_wfs.contents = {"test:layer": mock_layer}
        mock_wfs.getOperationByName.return_value = mock_get_feature

        with (
            patch("udata_hydra.analysis.ogc.config") as mock_config,
            patch("udata_hydra.analysis.ogc.WebFeatureService", return_value=mock_wfs),
            patch("udata_hydra.analysis.ogc.helpers.notify_udata", new_callable=AsyncMock) as mock_notify,
        ):
            mock_config.OGC_ANALYSIS_ENABLED = True
            result = await analyse_ogc(check)

        expected_metadata = {
            "format": "wfs",
            # first version tested
            "version": "2.0.0",
            "feature_types": [
                {
                    "name": "test:layer",
                    "default_crs": "EPSG:4326",
                    "other_crs": ["EPSG:3857"],
                }
            ],
            "output_formats": ["application/json", "text/xml"],
        }
        assert result == expected_metadata

        # Verify metadata was stored in the database
        res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
        assert json.loads(res["ogc_metadata"]) == expected_metadata
        assert res["parsing_started_at"] is not None
        assert res["parsing_finished_at"] is not None

        # Verify notify_udata was called
        mock_notify.assert_called_once()

    async def test_analyse_wfs_connection_error(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        with (
            patch("udata_hydra.analysis.ogc.config") as mock_config,
            patch(
                "udata_hydra.analysis.ogc.WebFeatureService",
                side_effect=RequestsConnectionError("Connection failed"),
            ),
            patch("udata_hydra.analysis.ogc.helpers.notify_udata", new_callable=AsyncMock) as mock_notify,
        ):
            mock_config.OGC_ANALYSIS_ENABLED = True
            result = await analyse_ogc(check)

        assert result is None

        # Verify error was stored in the database via handle_parse_exception
        res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
        assert res["parsing_error"] is not None
        assert "wfs_connection" in res["parsing_error"]
        assert res["parsing_started_at"] is not None
        assert res["parsing_finished_at"] is not None

        # Verify notify_udata was called
        mock_notify.assert_called_once()

    async def test_analyse_wfs_fallback_version(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_crs_4326 = MagicMock()
        mock_crs_4326.getcode.return_value = "EPSG:4326"

        mock_layer = MagicMock()
        mock_layer.crsOptions = [mock_crs_4326]

        mock_wfs = MagicMock()
        mock_wfs.contents = {"layer1": mock_layer}
        mock_wfs.getOperationByName.return_value = None

        call_count = 0

        def wfs_side_effect(*args, **kwargs):
            """First call will raise exception, second one passes"""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ServiceException("Version not supported")
            return mock_wfs

        with (
            patch("udata_hydra.analysis.ogc.config") as mock_config,
            patch("udata_hydra.analysis.ogc.WebFeatureService", side_effect=wfs_side_effect),
            patch("udata_hydra.analysis.ogc.helpers.notify_udata", new_callable=AsyncMock),
        ):
            mock_config.OGC_ANALYSIS_ENABLED = True
            result = await analyse_ogc(check)

        assert result is not None
        # second version tested
        assert result["version"] == "1.1.0"

    async def test_analyse_wfs_empty_contents(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_wfs = MagicMock()
        mock_wfs.contents = {}
        mock_wfs.getOperationByName.return_value = None

        with (
            patch("udata_hydra.analysis.ogc.config") as mock_config,
            patch("udata_hydra.analysis.ogc.WebFeatureService", return_value=mock_wfs),
            patch("udata_hydra.analysis.ogc.helpers.notify_udata", new_callable=AsyncMock),
        ):
            mock_config.OGC_ANALYSIS_ENABLED = True
            result = await analyse_ogc(check)

        assert result == {
            "format": "wfs",
            "version": "2.0.0",
            "feature_types": [],
            "output_formats": [],
        }

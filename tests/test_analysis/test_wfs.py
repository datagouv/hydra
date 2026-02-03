import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from owslib.util import ServiceException
from requests.exceptions import ConnectionError as RequestsConnectionError

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.wfs import analyse_wfs
from udata_hydra.utils.wfs import detect_wfs_from_url

log = logging.getLogger("udata-hydra")


class TestWfsDetection:
    """Tests for WFS URL detection"""

    def test_detect_wfs_service_param_uppercase(self):
        check = {"url": "https://example.com/geoserver?SERVICE=WFS&REQUEST=GetCapabilities"}
        assert detect_wfs_from_url(check) is True

    def test_detect_wfs_service_param_lowercase(self):
        check = {"url": "https://example.com/geoserver?service=wfs&request=GetCapabilities"}
        assert detect_wfs_from_url(check) is True

    def test_detect_wfs_service_param_mixed_case(self):
        check = {"url": "https://example.com/geoserver?Service=Wfs&Request=GetCapabilities"}
        assert detect_wfs_from_url(check) is True

    def test_detect_wfs_path(self):
        check = {"url": "https://example.com/geoserver/wfs"}
        assert detect_wfs_from_url(check) is True

    def test_detect_wfs_path_uppercase(self):
        check = {"url": "https://example.com/geoserver/WFS"}
        assert detect_wfs_from_url(check) is True

    def test_detect_wfs_path_with_query(self):
        check = {"url": "https://example.com/geoserver/wfs?request=GetCapabilities"}
        assert detect_wfs_from_url(check) is True

    def test_detect_non_wfs_url(self):
        check = {"url": "https://example.com/data/file.csv"}
        assert detect_wfs_from_url(check) is False

    def test_detect_wms_not_wfs(self):
        check = {"url": "https://example.com/geoserver?SERVICE=WMS&REQUEST=GetCapabilities"}
        assert detect_wfs_from_url(check) is False

    def test_detect_empty_url(self):
        check = {"url": ""}
        assert detect_wfs_from_url(check) is False

    def test_detect_missing_url(self):
        check = {}
        assert detect_wfs_from_url(check) is False


class TestWfsAnalysis:
    """Tests for WFS analysis"""

    @pytest.mark.asyncio
    async def test_analyse_wfs_disabled(self, setup_catalog, fake_check):
        with patch("udata_hydra.analysis.wfs.config") as mock_config:
            mock_config.WFS_ANALYSIS_ENABLED = False
            check = await fake_check()
            result = await analyse_wfs(check)
            assert result is None

    @pytest.mark.asyncio
    async def test_analyse_wfs_success(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_layer = MagicMock()
        mock_layer.crsOptions = ["EPSG:4326", "EPSG:3857"]

        mock_get_feature = MagicMock()
        mock_get_feature.parameters = {
            "outputFormat": {"values": ["application/json", "text/xml"]}
        }

        mock_wfs = MagicMock()
        mock_wfs.contents = {"test:layer": mock_layer}
        mock_wfs.getOperationByName.return_value = mock_get_feature

        with (
            patch("udata_hydra.analysis.wfs.config") as mock_config,
            patch("udata_hydra.analysis.wfs.WebFeatureService", return_value=mock_wfs),
            patch("udata_hydra.analysis.wfs.helpers.notify_udata", new_callable=AsyncMock) as mock_notify,
        ):
            mock_config.WFS_ANALYSIS_ENABLED = True
            mock_config.WFS_GETCAPABILITIES_TIMEOUT = 30

            result = await analyse_wfs(check)

        assert result is not None
        assert result["version"] == "2.0.0"
        assert "title" not in result
        assert len(result["feature_types"]) == 1
        assert result["feature_types"][0]["name"] == "test:layer"
        assert "title" not in result["feature_types"][0]
        assert result["feature_types"][0]["default_crs"] == "EPSG:4326"
        assert result["feature_types"][0]["other_crs"] == ["EPSG:3857"]
        assert result["output_formats"] == ["application/json", "text/xml"]

        # Verify metadata was stored in the database
        res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
        assert res["wfs_metadata"] is not None
        assert res["parsing_started_at"] is not None
        assert res["parsing_finished_at"] is not None

        # Verify notify_udata was called
        mock_notify.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyse_wfs_connection_error(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        with (
            patch("udata_hydra.analysis.wfs.config") as mock_config,
            patch(
                "udata_hydra.analysis.wfs.WebFeatureService",
                side_effect=RequestsConnectionError("Connection failed"),
            ),
            patch("udata_hydra.analysis.wfs.helpers.notify_udata", new_callable=AsyncMock) as mock_notify,
        ):
            mock_config.WFS_ANALYSIS_ENABLED = True
            mock_config.WFS_GETCAPABILITIES_TIMEOUT = 30

            result = await analyse_wfs(check)

        assert result is None

        # Verify error was stored in the database via handle_parse_exception
        res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
        assert res["parsing_error"] is not None
        assert "wfs_connection" in res["parsing_error"]
        assert res["parsing_started_at"] is not None
        assert res["parsing_finished_at"] is not None

        # Verify notify_udata was called
        mock_notify.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyse_wfs_fallback_version(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_layer = MagicMock()
        mock_layer.crsOptions = ["EPSG:4326"]

        mock_wfs = MagicMock()
        mock_wfs.contents = {"layer1": mock_layer}
        mock_wfs.getOperationByName.return_value = None

        call_count = 0

        def wfs_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ServiceException("Version not supported")
            return mock_wfs

        with (
            patch("udata_hydra.analysis.wfs.config") as mock_config,
            patch("udata_hydra.analysis.wfs.WebFeatureService", side_effect=wfs_side_effect),
            patch("udata_hydra.analysis.wfs.helpers.notify_udata", new_callable=AsyncMock),
        ):
            mock_config.WFS_ANALYSIS_ENABLED = True
            mock_config.WFS_GETCAPABILITIES_TIMEOUT = 30

            result = await analyse_wfs(check)

        assert result is not None
        assert result["version"] == "1.1.0"

    @pytest.mark.asyncio
    async def test_analyse_wfs_empty_contents(self, setup_catalog, db, fake_check):
        check = await fake_check(url="https://example.com/geoserver/wfs?SERVICE=WFS")

        mock_wfs = MagicMock()
        mock_wfs.contents = {}
        mock_wfs.getOperationByName.return_value = None

        with (
            patch("udata_hydra.analysis.wfs.config") as mock_config,
            patch("udata_hydra.analysis.wfs.WebFeatureService", return_value=mock_wfs),
            patch("udata_hydra.analysis.wfs.helpers.notify_udata", new_callable=AsyncMock),
        ):
            mock_config.WFS_ANALYSIS_ENABLED = True
            mock_config.WFS_GETCAPABILITIES_TIMEOUT = 30

            result = await analyse_wfs(check)

        assert result is not None
        assert result["feature_types"] == []
        assert result["output_formats"] == []

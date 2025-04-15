import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from shapely.geometry import Point

from udata_hydra.analysis.geojson import geojson_to_pmtiles


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_disabled():
    """Test that the function returns None when GEOJSON_TO_PMTILES is False"""
    with patch("udata_hydra.config.GEOJSON_TO_PMTILES", False):
        result = await geojson_to_pmtiles("test.geojson")
        assert result is None


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_success():
    """Test successful conversion of GeoJSON to PMTiles"""
    # Mock data
    mock_features = [
        {"geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {"name": "test1"}},
        {"geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {"name": "test2"}},
    ]

    # Mock Fiona
    mock_src = MagicMock()
    mock_src.__len__.return_value = 2
    mock_src.__iter__.return_value = mock_features

    # Mock PMTiles writer
    mock_writer = MagicMock()

    # Mock MinIO client
    mock_minio = MagicMock()
    mock_minio.send_file.return_value = "http://minio/test.pmtiles"

    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch("udata_hydra.config.MIN_FEATURES_FOR_PMTILES", 1),
        patch("fiona.open", return_value=mock_src),
        patch("udata_hydra.analysis.geojson.pmtiles.Writer", return_value=mock_writer),
        patch("udata_hydra.analysis.geojson.MinIOClient", return_value=mock_minio),
        patch("os.path.getsize", return_value=1024),
        patch("os.remove"),
    ):
        result = await geojson_to_pmtiles("test.geojson")

        # Verify results
        assert result == ("http://minio/test.pmtiles", 1024)

        # Verify PMTiles writer was used correctly
        assert mock_writer.add_feature.call_count == 2
        mock_writer.write.assert_called_once()

        # Verify MinIO upload
        mock_minio.send_file.assert_called_once()

        # Verify cleanup
        os.remove.assert_called_once()


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_with_resource_id():
    """Test that resource status is updated when resource_id is provided"""
    # Mock data
    mock_features = [{"geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}]
    mock_src = MagicMock()
    mock_src.__len__.return_value = 1
    mock_src.__iter__.return_value = mock_features

    # Mock Resource.update
    mock_update = AsyncMock()

    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch("udata_hydra.config.MIN_FEATURES_FOR_PMTILES", 1),
        patch("fiona.open", return_value=mock_src),
        patch("udata_hydra.analysis.geojson.pmtiles.Writer", return_value=MagicMock()),
        patch("udata_hydra.analysis.geojson.MinIOClient", return_value=MagicMock()),
        patch("os.path.getsize", return_value=1024),
        patch("os.remove"),
        patch("udata_hydra.analysis.geojson.Resource.update", mock_update),
    ):
        await geojson_to_pmtiles("test.geojson", resource_id="test-resource")

        # Verify resource status was updated
        mock_update.assert_called_once_with("test-resource", {"status": "CONVERTING_TO_PMTILES"})


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_invalid_geometry():
    """Test handling of invalid geometry"""
    # Mock data with invalid geometry
    mock_features = [{"geometry": None, "properties": {}}]
    mock_src = MagicMock()
    mock_src.__len__.return_value = 1
    mock_src.__iter__.return_value = mock_features

    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch("udata_hydra.config.MIN_FEATURES_FOR_PMTILES", 1),
        patch("fiona.open", return_value=mock_src),
        patch("udata_hydra.analysis.geojson.pmtiles.Writer", return_value=MagicMock()),
        patch("udata_hydra.analysis.geojson.MinIOClient", return_value=MagicMock()),
        patch("os.path.getsize", return_value=1024),
        patch("os.remove"),
    ):
        with pytest.raises(Exception):
            await geojson_to_pmtiles("test.geojson")

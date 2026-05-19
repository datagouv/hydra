import os
import tempfile
from unittest.mock import MagicMock

import pytest

from udata_hydra.analysis import helpers
from udata_hydra.utils import IOException


@pytest.mark.asyncio
async def test_read_or_download_filename_resolution(mocker):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", "/tmp")
    temp_dir = "/tmp"
    os.makedirs(temp_dir, exist_ok=True)

    # Create a temporary file in the configured temp directory
    with tempfile.NamedTemporaryFile(mode="w", delete=False, dir=temp_dir) as tmp_file:
        tmp_file.write("test content")
        tmp_file_path = tmp_file.name

    try:
        mock_check = MagicMock()
        mock_check["resource_id"] = "test_resource"
        mock_check["url"] = "http://example.com/test.csv"

        basename = os.path.basename(tmp_file_path)
        result = await helpers.read_or_download_file(
            check=mock_check, filename=basename, file_format="csv", exception=None
        )

        assert result.read() == b"test content"
        result.close()

    finally:
        # Clean up
        os.unlink(tmp_file_path)


@pytest.mark.asyncio
async def test_read_or_download_file_missing_file():
    """Test that read_or_download_file raises IOException for missing files"""

    # Create a mock check record
    mock_check = MagicMock()
    mock_check["resource_id"] = "test_resource"
    mock_check["url"] = "http://example.com/test.csv"

    with pytest.raises(IOException) as exc_info:
        await helpers.read_or_download_file(
            check=mock_check,
            filename="non_existent_file.csv",
            file_format="csv",
            exception=None,
        )

    assert "Temporary file not found" in str(exc_info.value)

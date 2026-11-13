import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from udata_hydra.analysis import helpers
from udata_hydra.data_formats import Csv
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
            check=mock_check, filename=basename, data_format=Csv, exception=None
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
            data_format=Csv,
            exception=None,
        )

    assert "Temporary file not found" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing",
    ("check", "resource"),
)
async def test_notify_udata_raises_without_check_or_resource(missing):
    resource = {"dataset_id": "dataset-id"}
    check = {
        "resource_id": "resource-id",
        "parsing_started_at": datetime.now(timezone.utc),
        "parsing_finished_at": datetime.now(timezone.utc),
    }
    if missing == "check":
        check = None
    else:
        resource = None

    with pytest.raises(ValueError, match="Tried to notify udata"):
        await helpers.notify_udata(resource, check)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config_flag,check_fields,expected_document_keys",
    (
        (
            "CSV_TO_DB",
            {"parsing_table": "tbl_abc"},
            ["analysis:parsing:parsing_table"],
        ),
        (
            "DB_TO_PARQUET",
            {"parquet_url": "https://example.com/file.parquet", "parquet_size": 42},
            ["analysis:parsing:parquet_url", "analysis:parsing:parquet_size"],
        ),
        (
            "GEOJSON_TO_PMTILES",
            {"pmtiles_url": "https://example.com/file.pmtiles", "pmtiles_size": 99},
            ["analysis:parsing:pmtiles_url", "analysis:parsing:pmtiles_size"],
        ),
        (
            "DB_TO_GEOJSON",
            {"geojson_url": "https://example.com/file.geojson", "geojson_size": 77},
            ["analysis:parsing:geojson_url", "analysis:parsing:geojson_size"],
        ),
        (
            "CODE_COMMUNE_ANALYSIS_ENABLED",
            {"code_commune_values": {"code_commune": ["13055", "75056"]}},
            ["analysis:parsing:code_commune_values"],
        ),
    ),
)
async def test_notify_udata_includes_optional_payload_fields(
    mocker, config_flag, check_fields, expected_document_keys
):
    enqueue = mocker.patch("udata_hydra.analysis.helpers.queue.enqueue")
    mocker.patch(f"udata_hydra.config.{config_flag}", True)

    started_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    finished_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
    check = {
        "resource_id": "resource-id",
        "parsing_started_at": started_at,
        "parsing_finished_at": finished_at,
        **check_fields,
    }
    resource = {"dataset_id": "dataset-id"}

    await helpers.notify_udata(resource, check)

    enqueue.assert_called_once()
    document = enqueue.call_args.kwargs["document"].payload
    for key in expected_document_keys:
        assert key in document


@pytest.mark.asyncio
async def test_notify_udata_parses_string_ogc_metadata(mocker):
    enqueue = mocker.patch("udata_hydra.analysis.helpers.queue.enqueue")
    mocker.patch("udata_hydra.config.OGC_ANALYSIS_ENABLED", True)

    ogc_metadata = {"service": "WMS", "version": "1.3.0"}
    check = {
        "resource_id": "resource-id",
        "parsing_started_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "parsing_finished_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "ogc_metadata": json.dumps(ogc_metadata),
    }
    resource = {"dataset_id": "dataset-id"}

    await helpers.notify_udata(resource, check)

    document = enqueue.call_args.kwargs["document"].payload
    assert document["analysis:parsing:ogc_metadata"] == ogc_metadata


@pytest.mark.asyncio
async def test_notify_udata_parses_string_code_commune_values(mocker):
    enqueue = mocker.patch("udata_hydra.analysis.helpers.queue.enqueue")
    mocker.patch("udata_hydra.config.CODE_COMMUNE_ANALYSIS_ENABLED", True)

    code_commune_values = {"code_commune": ["13055", "75056"]}
    check = {
        "resource_id": "resource-id",
        "parsing_started_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "parsing_finished_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "code_commune_values": json.dumps(code_commune_values),
    }
    resource = {"dataset_id": "dataset-id"}

    await helpers.notify_udata(resource, check)

    document = enqueue.call_args.kwargs["document"].payload
    assert document["analysis:parsing:code_commune_values"] == code_commune_values

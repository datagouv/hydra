"""Unit tests for S3Client (object key prefix / public URL)."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from udata_hydra.utils.s3 import CONTENT_TYPES, S3Client


@pytest.fixture
def mock_s3(mocker: MockerFixture) -> Iterator[MagicMock]:
    mocker.patch("udata_hydra.config.S3_ENDPOINT", "s3-example.com")
    resource = MagicMock()
    resource.meta.client.head_bucket.return_value = {}
    bucket = MagicMock()
    resource.Bucket.return_value = bucket
    with patch("udata_hydra.utils.s3.boto3.resource", return_value=resource):
        yield bucket


def test_s3_client_upload_at_bucket_root(mock_s3: MagicMock, tmp_path: Path) -> None:
    f = tmp_path / "file.parquet"
    f.write_bytes(b"x")
    client = S3Client(bucket="my-bucket")
    url = client.send_file(f, delete_source=False)

    mock_s3.upload_file.assert_called_once_with(
        str(f),
        "file.parquet",
        ExtraArgs={
            "ContentType": CONTENT_TYPES[f.name.split(".")[-1]],
            "ACL": "public-read",
        },
    )
    assert url == "https://s3-example.com/my-bucket/file.parquet"


def test_s3_client_upload_with_prefix(mock_s3: MagicMock, tmp_path: Path) -> None:
    f = tmp_path / "file.geojson"
    f.write_bytes(b"x")
    client = S3Client(bucket="my-bucket", prefix="exports")
    url = client.send_file(f, delete_source=False)

    mock_s3.upload_file.assert_called_once_with(
        str(f),
        "exports/file.geojson",
        ExtraArgs={
            "ContentType": CONTENT_TYPES[f.name.split(".")[-1]],
            "ACL": "public-read",
        },
    )
    assert url == "https://s3-example.com/my-bucket/exports/file.geojson"


def test_s3_client_upload_changed_pattern(mock_s3: MagicMock, tmp_path: Path, mocker) -> None:
    f = tmp_path / "file.pmtiles"
    f.write_bytes(b"x")
    mocker.patch("udata_hydra.config.S3_URL_PATTERN", "s3://{bucket}:{endpoint}:{key}")
    client = S3Client(bucket="my-bucket", prefix="exports")
    url = client.send_file(f, delete_source=False)

    mock_s3.upload_file.assert_called_once_with(
        str(f),
        "exports/file.pmtiles",
        ExtraArgs={
            "ContentType": CONTENT_TYPES[f.name.split(".")[-1]],
            "ACL": "public-read",
        },
    )
    assert url == "s3://my-bucket:s3-example.com:exports/file.pmtiles"

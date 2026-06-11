"""Unit tests for S3Client (object key prefix / public URL)."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pytest_mock import MockerFixture

from udata_hydra import config
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


@pytest.mark.parametrize(
    "extension,patched_config",
    (
        ("parquet", None),
        ("geojson", None),
        ("pmtiles", ("udata_hydra.config.S3_URL_PATTERN", "s3://{bucket}:{endpoint}:{key}")),
    ),
)
def test_s3_client_upload(
    mock_s3: MagicMock, tmp_path: Path, extension: str, patched_config: tuple[str, str], mocker
) -> None:
    f = tmp_path / f"file.{extension}"
    f.write_bytes(b"x")
    if patched_config:
        mocker.patch(*patched_config)
    client = S3Client(bucket="my-bucket")
    url = client.send_file(f, delete_source=False)

    mock_s3.upload_file.assert_called_once_with(
        str(f),
        f"{extension}/file.{extension}",
        ExtraArgs={
            "ContentType": CONTENT_TYPES[extension],
            "ACL": "public-read",
        },
    )
    assert url == config.S3_URL_PATTERN.format(
        endpoint="s3-example.com",
        bucket="my-bucket",
        key=f"{extension}/file.{extension}",
    )


def test_s3_client_raises_when_bucket_does_not_exist(mocker: MockerFixture) -> None:
    mocker.patch("udata_hydra.config.S3_ENDPOINT", "s3-example.com")
    resource = MagicMock()
    resource.meta.client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "Not found"}},
        "HeadBucket",
    )
    with patch("udata_hydra.utils.s3.boto3.resource", return_value=resource):
        with pytest.raises(ValueError, match="does not exist"):
            S3Client(bucket="missing-bucket")


def test_s3_client_send_file_requires_bucket(tmp_path: Path) -> None:
    client = S3Client.__new__(S3Client)
    client.bucket = None
    with pytest.raises(AttributeError, match="bucket has to be specified"):
        client.send_file(tmp_path / "missing.parquet")


def test_s3_client_send_file_raises_when_file_missing(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    mocker.patch("udata_hydra.config.S3_ENDPOINT", "s3-example.com")
    resource = MagicMock()
    resource.meta.client.head_bucket.return_value = {}
    with patch("udata_hydra.utils.s3.boto3.resource", return_value=resource):
        client = S3Client(bucket="my-bucket")
        missing_file = tmp_path / "missing.geojson"
        with pytest.raises(Exception, match="does not exists"):
            client.send_file(missing_file)

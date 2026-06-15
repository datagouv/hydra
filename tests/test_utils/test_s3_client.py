"""Unit tests for S3Client (object key prefix / public URL)."""

import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pytest_mock import MockerFixture

from udata_hydra import config
from udata_hydra.context import context
from udata_hydra.data_formats import DataFormat, Geojson, Parquet, PMTiles
from udata_hydra.utils import true_path
from udata_hydra.utils.s3 import S3Client


@pytest.fixture(autouse=True)
def _reset_s3_client_cache() -> Iterator[None]:
    yield
    context.pop("s3", None)


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
    "data_format,patched_config",
    (
        (Parquet, None),
        (Geojson, None),
        (PMTiles, ("udata_hydra.config.S3_URL_PATTERN", "s3://{bucket}:{endpoint}:{key}")),
    ),
)
def test_s3_client_upload(
    mock_s3: MagicMock,
    data_format: DataFormat,
    patched_config: tuple[str, str],
    mocker,
) -> None:
    extension = data_format.__class__.__name__.lower()
    f = Path(true_path(f"file.{extension}"))
    f.write_bytes(b"x")
    file = data_format(file_name=os.path.basename(f.name))
    if patched_config:
        mocker.patch(*patched_config)
    client = S3Client(bucket="my-bucket")
    url = client.send_file(file, delete_source=False)

    mock_s3.upload_file.assert_called_once_with(
        str(f),
        f"{extension}/file.{extension}",
        ExtraArgs={
            "ContentType": file.standard_mime_type,
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


def test_s3_client_send_file_raises_when_file_missing(mocker: MockerFixture) -> None:
    mocker.patch("udata_hydra.config.S3_ENDPOINT", "s3-example.com")
    resource = MagicMock()
    resource.meta.client.head_bucket.return_value = {}
    with patch("udata_hydra.utils.s3.boto3.resource", return_value=resource):
        client = S3Client(bucket="my-bucket")
        with patch("udata_hydra.data_formats.data_format.os.path.getsize", return_value=10):
            missing_file = Geojson(file_name="missing.geojson")
        with pytest.raises(Exception, match="does not exists"):
            client.send_file(missing_file)

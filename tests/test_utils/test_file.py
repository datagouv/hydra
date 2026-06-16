import gzip
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Geojson
from udata_hydra.utils import IOException
from udata_hydra.utils.file import download_resource, extract_gzip, remove_remainders, storage_path


@pytest.mark.parametrize("case", ("configured", "system-temp", "test-data"))
def test_storage_path(mocker, tmp_path, case):
    if case == "configured":
        mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
        assert storage_path("file.csv") == tmp_path / "file.csv"
        assert storage_path("") == tmp_path
    elif case == "system-temp":
        mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", "")
        assert storage_path("file.csv").parent == Path(tempfile.gettempdir())
    else:
        assert storage_path("tests/data/foo.csv") == Path("tests/data/foo.csv")


def test_extract_gzip_writes_to_storage_path(mocker, tmp_path):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
    gz_path = tmp_path / "data.csv.gz"
    gz_path.write_bytes(gzip.compress(b"col\nval"))

    extracted = extract_gzip(str(gz_path))

    try:
        assert Path(extracted.name).parent == storage_path("")
        with open(extracted.name, "rb") as f:
            assert f.read() == b"col\nval"
    finally:
        os.remove(extracted.name)


async def test_download_resource_rejects_oversized_content_length():
    with pytest.raises(IOException, match="File too large to download"):
        await download_resource(
            url="http://example.com/file.csv",
            headers={"content-length": "1000"},
            max_size_allowed=500,
        )


async def test_download_resource_rejects_oversized_while_streaming(rmock):
    url = "http://example.com/file.csv"
    rmock.get(url, status=200, body=b"x" * 2048)

    with pytest.raises(IOException, match="File too large to download"):
        await download_resource(url=url, max_size_allowed=500)


async def test_remove_remainders():
    def _crash(*args, **kwargs):
        raise Exception("BOOM")

    geojson_file = Geojson(file_name="tests/data/valid.geojson", resource_id=RESOURCE_ID)
    with patch(
        "udata_hydra.data_formats.PMTiles",
        new=_crash,
    ):
        with pytest.raises(Exception):
            await geojson_file.to_pmtiles()
    assert storage_path(f"{RESOURCE_ID}.pmtiles").exists()
    remove_remainders(RESOURCE_ID, ["pmtiles"])
    assert not storage_path(f"{RESOURCE_ID}.pmtiles").exists()

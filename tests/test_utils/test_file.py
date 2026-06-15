import gzip
import os
import tempfile
from pathlib import Path

import pytest

from udata_hydra.utils import IOException
from udata_hydra.utils.file import (
    download_resource,
    extract_gzip,
    remove_remainders,
    temporary_folder,
)


@pytest.mark.parametrize(
    "temp_config",
    (
        pytest.param("configured", id="configured"),
        pytest.param("", id="system-temp"),
    ),
)
def test_remove_remainders_cleans_temporary_folder(mocker, tmp_path, temp_config):
    if temp_config == "configured":
        mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
        expected_parent = tmp_path
    else:
        mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", "")
        expected_parent = Path(tempfile.gettempdir())

    resource_id = "deadbeef"
    leftover = expected_parent / f"{resource_id}.parquet"
    leftover.write_text("leftover")

    try:
        remove_remainders(resource_id, ["parquet"])
        assert not leftover.exists()
        assert temporary_folder() == expected_parent
    finally:
        leftover.unlink(missing_ok=True)


def test_remove_remainders_ignores_cwd(mocker, tmp_path):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
    cwd_file = Path("orphan.parquet")
    cwd_file.write_text("keep me")

    try:
        remove_remainders("orphan", ["parquet"])
        assert cwd_file.exists()
    finally:
        cwd_file.unlink(missing_ok=True)


def test_extract_gzip_writes_to_temporary_folder(mocker, tmp_path):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
    gz_path = tmp_path / "data.csv.gz"
    with gzip.open(gz_path, "wb") as gz_file:
        gz_file.write(b"col\nval")

    extracted = extract_gzip(str(gz_path))

    try:
        assert Path(extracted.name).parent == tmp_path
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

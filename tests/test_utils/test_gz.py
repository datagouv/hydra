import gzip

import magic
import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Gz
from udata_hydra.utils import IOException


def test_gz_unwrap_extracts_payload(mocker, tmp_path):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
    gz_path = tmp_path / "data.csv.gz"
    gz_path.write_bytes(gzip.compress(b"col\nval"))

    file = Gz(file_name=gz_path.name, resource_id=RESOURCE_ID)
    file.unwrap()

    try:
        assert not gz_path.exists()
        assert file.path.read_bytes() == b"col\nval"
        assert magic.from_file(str(file.path), mime=True) not in (
            "application/gzip",
            "application/x-gzip",
        )
    finally:
        file.path.unlink(missing_ok=True)


def test_gz_unwrap_corrupted_gzip_raises_ioerror(mocker, tmp_path):
    mocker.patch("udata_hydra.config.TEMPORARY_DOWNLOAD_FOLDER", str(tmp_path))
    gz_path = tmp_path / "truncated.csv.gz"
    gz_path.write_bytes(gzip.compress(b"col\nval")[:10])

    file = Gz(file_name=gz_path.name, resource_id=RESOURCE_ID)
    with pytest.raises(IOException, match="Corrupted or truncated gzip file"):
        file.unwrap()

    assert not list(tmp_path.iterdir())

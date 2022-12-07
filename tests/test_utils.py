import hashlib
import os
import tempfile

import pytest

from udata_hydra.utils.csv import find_delimiter, detect_encoding
from udata_hydra.utils.file import compute_checksum_from_file


def test_detect_encoding():
    with pytest.raises(FileNotFoundError):
        detect_encoding("not-a-real-file-hopefully.txt")
    encoding = detect_encoding("tests/catalog.csv")
    assert encoding is not None
    encoding = detect_encoding("docs/screenshot.png")
    assert encoding is None


def test_find_delimiter():
    with pytest.raises(FileNotFoundError):
        find_delimiter("not-a-real-file-hopefully.txt")
    delimiter = find_delimiter("tests/catalog.csv")
    assert delimiter == ";"
    delimiter = find_delimiter("docs/screenshot.png")
    assert delimiter is None
    delimiter = find_delimiter("tests/catalog.csv", encoding="nimp")
    assert delimiter is None


def test_compute_checksum_from_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(b"a very small file")
    tmp_file.close()

    checksum = compute_checksum_from_file(tmp_file.name)
    assert checksum == hashlib.sha1(b"a very small file").hexdigest()
    os.remove(tmp_file.name)

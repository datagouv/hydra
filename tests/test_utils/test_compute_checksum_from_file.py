import hashlib
import os
import tempfile

from udata_hydra.utils import compute_checksum_from_file


def test_compute_checksum_from_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(b"a very small file")
    tmp_file.close()

    checksum = compute_checksum_from_file(tmp_file.name)
    assert checksum == hashlib.sha1(b"a very small file").hexdigest()
    os.remove(tmp_file.name)

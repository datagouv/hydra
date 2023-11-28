import hashlib
import logging
import tempfile

from typing import BinaryIO

import aiohttp

from udata_hydra import config

log = logging.getLogger("udata-hydra")


def compute_checksum_from_file(filename):
    """Compute sha1 in blocks"""
    sha1sum = hashlib.sha1()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = f.read(2**16)
    return sha1sum.hexdigest()


async def download_resource(url: str, headers: dict, ignore_size: bool = False) -> BinaryIO:
    """
    Attempts downloading a resource from a given url.
    Returns the downloaded file object.
    Raises IOError if the resource is too large.
    """
    tmp_file = tempfile.NamedTemporaryFile(dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, delete=False)

    if not ignore_size and float(headers.get("content-length", -1)) > float(config.MAX_FILESIZE_ALLOWED):
        raise IOError("File too large to download")

    chunk_size = 1024
    i = 0
    async with aiohttp.ClientSession(headers={"user-agent": config.USER_AGENT}, raise_for_status=True) as session:
        async with session.get(url, allow_redirects=True) as response:
            async for chunk in response.content.iter_chunked(chunk_size):
                if ignore_size or i * chunk_size < float(config.MAX_FILESIZE_ALLOWED):
                    tmp_file.write(chunk)
                else:
                    tmp_file.close()
                    log.warning(f"File {url} is too big, skipping")
                    raise IOError("File too large to download")
                i += 1
    tmp_file.close()
    return tmp_file

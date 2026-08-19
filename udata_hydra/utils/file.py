import gzip
import hashlib
import logging
import mimetypes
import re
import tempfile
from pathlib import Path
from typing import IO

import aiohttp
import magic

from udata_hydra import config
from udata_hydra.utils.errors import IOException
from udata_hydra.utils.http import get_http_client

log = logging.getLogger("udata-hydra")


def storage_path(file_name: str) -> Path:
    if file_name.startswith("tests/data/"):
        return Path(file_name)
    return Path(config.TEMPORARY_DOWNLOAD_FOLDER or tempfile.gettempdir()) / file_name


def compute_checksum_from_file(filename: str) -> str:
    """Compute sha1 in blocks"""
    sha1sum = hashlib.sha1()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = f.read(2**16)
    return sha1sum.hexdigest()


def extract_gzip(file_path: str, url: str | None = None) -> IO[bytes]:
    temp_file = None
    try:
        with gzip.open(file_path, "rb") as gz_file:
            with tempfile.NamedTemporaryFile(
                dir=storage_path(""), mode="wb", delete=False
            ) as temp_file:
                temp_file.write(gz_file.read())
    except (EOFError, gzip.BadGzipFile) as e:
        if temp_file is not None:
            Path(temp_file.name).unlink(missing_ok=True)
        raise IOException("Corrupted or truncated gzip file", url=url) from e
    return temp_file


async def download_resource(
    url: str,
    headers: dict | None = None,
    max_size_allowed: int | None = None,
) -> tuple[IO[bytes], str]:
    """
    Attempts downloading a resource from a given url.
    Returns a tuple of (downloaded_file_object, detected_extension).
    Raises custom IOException if the resource is too large or if the URL is unreachable.
    """
    if (
        headers
        and max_size_allowed is not None
        and float(headers.get("content-length", -1)) > max_size_allowed
    ):
        raise IOException("File too large to download", url=url)

    tmp_file = tempfile.NamedTemporaryFile(dir=storage_path(""), delete=False)

    chunk_size = 1024
    i = 0
    too_large, download_error = False, None
    try:
        session = await get_http_client()
        async with session.get(url, allow_redirects=True) as response:
            async for chunk in response.content.iter_chunked(chunk_size):
                if max_size_allowed is None or i * chunk_size < max_size_allowed:
                    tmp_file.write(chunk)
                else:
                    too_large = True
                    break
                i += 1
    except aiohttp.ClientResponseError as e:
        download_error = e
    finally:
        tmp_file.close()
        if too_large or download_error:
            Path(tmp_file.name).unlink(missing_ok=True)
            if too_large:
                raise IOException("File too large to download", url=url)
            raise IOException("Error downloading CSV", url=url) from download_error

    detected_extension = ""

    if magic.from_file(tmp_file.name, mime=True) in [
        "application/x-gzip",
        "application/gzip",
    ]:
        # It's compressed - extract and determine extension from URL
        gzip_tmp_file_name = tmp_file.name
        try:
            tmp_file = extract_gzip(gzip_tmp_file_name, url=url)
        finally:
            Path(gzip_tmp_file_name).unlink(missing_ok=True)

        # Extract any extension before .gz using regex
        match = re.search(r"\.([^.]+)\.gz$", url)
        if match:
            detected_extension = f".{match.group(1)}"
        else:
            detected_extension = ""
    else:
        # Not compressed - use magic to detect type
        mime_type = magic.from_file(tmp_file.name, mime=True)
        detected_extension = mimetypes.guess_extension(mime_type) or ""

    return tmp_file, detected_extension


async def download_file(url: str, fd):
    """Download a file from URL to a file descriptor"""
    session = await get_http_client()
    async with session.get(url) as resp:
        while True:
            chunk = await resp.content.read(1024)
            if not chunk:
                break
            fd.write(chunk)


def remove_remainders(resource_id: str, extensions: list[str]) -> None:
    """Delete potential remainders from process that crashed"""
    for ext in extensions:
        storage_path(f"{resource_id}.{ext}").unlink(missing_ok=True)

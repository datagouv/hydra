import gzip
import hashlib
import logging
import mimetypes
import re
import tempfile
from pathlib import Path
from typing import IO, BinaryIO

import aiohttp
import magic

from udata_hydra import config
from udata_hydra.utils import IOException

log = logging.getLogger("udata-hydra")

HTTP_DOWNLOAD_CHUNK_SIZE = 1024


def compute_checksum_from_file(filename: str) -> str:
    """Compute sha1 in blocks"""
    sha1sum = hashlib.sha1()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = f.read(2**16)
    return sha1sum.hexdigest()


def extract_gzip(file_path: str | Path) -> IO[bytes]:
    with gzip.open(file_path, "rb") as gz_file:
        with tempfile.NamedTemporaryFile(
            dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, mode="wb", delete=False
        ) as temp_file:
            temp_file.write(gz_file.read())
    return temp_file


async def _http_get_to_temp_path(
    url: str,
    headers: dict | None,
    max_size_allowed: int | None,
    suffix: str,
    io_error_message: str,
) -> tuple[Path, int]:
    """Stream a GET response to a named temp file. Returns (path, total_bytes)."""
    if (
        headers
        and max_size_allowed is not None
        and float(headers.get("content-length", -1)) > max_size_allowed
    ):
        raise IOException("File too large to download", url=url)

    tmp_file: IO[bytes] = tempfile.NamedTemporaryFile(
        dir=config.TEMPORARY_DOWNLOAD_FOLDER or None,
        delete=False,
        suffix=suffix,
    )
    path = Path(tmp_file.name)
    total_bytes: int = 0
    too_large: bool = False
    download_error: aiohttp.ClientResponseError | None = None
    try:
        async with aiohttp.ClientSession(
            headers={"user-agent": config.USER_AGENT_FULL},
            raise_for_status=True,
        ) as session:
            async with session.get(url, allow_redirects=True) as response:
                async for chunk in response.content.iter_chunked(HTTP_DOWNLOAD_CHUNK_SIZE):
                    if max_size_allowed is None or total_bytes + len(chunk) <= max_size_allowed:
                        tmp_file.write(chunk)
                        total_bytes += len(chunk)
                    else:
                        too_large = True
                        break
    except aiohttp.ClientResponseError as e:
        download_error = e
    finally:
        tmp_file.close()
        if too_large or download_error:
            path.unlink(missing_ok=True)
        if too_large:
            raise IOException("File too large to download", url=url)
        if download_error:
            raise IOException(io_error_message, url=url) from download_error

    return path, total_bytes


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
    path, _ = await _http_get_to_temp_path(
        url,
        headers,
        max_size_allowed,
        "",
        "Error downloading CSV",
    )

    detected_extension: str = ""

    if magic.from_file(path, mime=True) in [
        "application/x-gzip",
        "application/gzip",
    ]:
        # It's compressed - extract and determine extension from URL
        tmp_file: IO[bytes] = extract_gzip(path)
        # Remove the gzip original temporary file
        path.unlink()

        # Extract any extension before .gz using regex
        match: re.Match[str] | None = re.search(r"\.([^.]+)\.gz$", url)
        if match:
            detected_extension = f".{match.group(1)}"
        else:
            detected_extension = ""
    else:
        # Not compressed - use magic to detect type
        mime_type: str = magic.from_file(path, mime=True)
        detected_extension = mimetypes.guess_extension(mime_type) or ""
        tmp_file = open(path, "rb")
        tmp_file.close()

    return tmp_file, detected_extension


async def download_url_to_tempfile(
    url: str,
    suffix: str = "",
    headers: dict | None = None,
    max_size_allowed: int | None = None,
) -> Path:
    """Download a URL to a named temporary file and return its path.

    Streams the response body to disk to avoid loading large files into memory.
    """
    path, total_bytes = await _http_get_to_temp_path(
        url,
        headers,
        max_size_allowed,
        suffix,
        "Error downloading file",
    )
    log.debug(f"Downloaded {url} to {path} ({total_bytes} bytes)")
    return path


async def download_file(url: str, fd: BinaryIO) -> None:
    """Download a file from URL to a file descriptor"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            while True:
                chunk: bytes = await resp.content.read(HTTP_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                fd.write(chunk)


def remove_remainders(resource_id: str, extensions: list[str]) -> None:
    """Delete potential remainders from process that crashed"""
    for ext in extensions:
        Path(f"{resource_id}.{ext}").unlink(missing_ok=True)

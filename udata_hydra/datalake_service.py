from datetime import datetime
import hashlib
import logging
import os
import tempfile
from typing import BinaryIO

import aiohttp
import magic
import pandas as pd

from udata_hydra import config, context
from udata_hydra.utils.http import send
from udata_hydra.utils.json import is_json_file
from udata_hydra.utils.minio import save_resource_to_minio
from udata_hydra.utils.csv import detect_encoding, find_delimiter


log = logging.getLogger("udata-hydra")


async def download_resource(url: str, session: aiohttp.ClientSession, headers: dict) -> BinaryIO:
    """
    Attempts downloading a resource from a given url.
    Returns the downloaded file object.
    Raises IOError if the resource is too large.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    if float(headers.get("content-length", -1)) > float(config.MAX_FILESIZE_ALLOWED):
        raise IOError("File too large to download")

    chunk_size = 1024
    i = 0
    # TODO: move to queue, can be long, also add setting for this
    timeout = aiohttp.ClientTimeout(total=5*60)
    async with session.get(url, timeout=timeout, allow_redirects=True) as response:
        async for chunk in response.content.iter_chunked(chunk_size):
            if i * chunk_size < float(config.MAX_FILESIZE_ALLOWED):
                tmp_file.write(chunk)
            else:
                tmp_file.close()
                log.error(f"File {url} is too big, skipping")
                raise IOError("File too large to download")
            i += 1
    tmp_file.close()
    return tmp_file


# TODO: we're sending analysis info to udata every time a resource is crawled
# although we only send "meta" check info only when they change
# maybe introduce conditionnal webhook trigger here too
async def process_resource(
    url: str,
    dataset_id: str,
    resource_id: str,
    session: aiohttp.ClientSession,
    headers: dict
) -> None:
    log.debug(f"Processing task for resource {resource_id} in dataset {dataset_id}")

    tmp_file = None
    mime_type = None
    sha1 = None
    filesize = None
    error = None

    try:
        tmp_file = await download_resource(url, session, headers)
        # Get file size
        filesize = os.path.getsize(tmp_file.name)

        # Get checksum
        sha1 = await compute_checksum_from_file(tmp_file.name)

        # Check resource MIME type
        # FIXME: this never seems to output text/csv, maybe override it later
        mime_type = magic.from_file(tmp_file.name, mime=True)
        if mime_type in ["text/plain", "text/csv", "application/csv"] and not is_json_file(tmp_file.name):
            # Save resource only if CSV
            try:
                # Try to detect encoding from suspected csv file. If fail, set up to utf8 (most common)
                with open(tmp_file.name, mode="rb") as f:
                    try:
                        encoding = detect_encoding(f)
                    # FIXME: catch exception more precisely
                    except Exception:
                        encoding = "utf-8"
                # Try to detect delimiter from suspected csv file. If fail, set up to None
                # (pandas will use python engine and try to guess separator itself)
                try:
                    delimiter = find_delimiter(tmp_file.name)
                # FIXME: catch exception more precisely
                except Exception:
                    delimiter = None

                # Try to read first 1000 rows with pandas
                pd.read_csv(tmp_file.name, sep=delimiter, encoding=encoding, nrows=1000)

                if config.SAVE_TO_MINIO:
                    save_resource_to_minio(tmp_file, dataset_id, resource_id)
                    storage_location = "/".join([
                        config.MINIO_URL,
                        config.MINIO_BUCKET,
                        config.MINIO_FOLDER,
                        dataset_id,
                        resource_id,
                    ])
                    log.debug(
                        f"Sending message to udata for resource stored {resource_id} in dataset {dataset_id}"
                    )
                    document = {"store:data_location": storage_location}
                    await send(dataset_id=dataset_id,
                               resource_id=resource_id,
                               document=document)
            except ValueError:
                log.warning(
                    f"Resource {resource_id} in dataset {dataset_id} is not a CSV"
                )

        # Send udata a message for both CSV and non CSV resources
        log.debug(
            f"Sending a message to udata for resource analysed {resource_id} in dataset {dataset_id}"
        )
        document = {
            "analysis:error": None,
            "analysis:filesize": filesize,
            "analysis:mime": mime_type,
        }
        # Check if checksum has been modified
        # TODO: improve file modification logic
        checksum_modified = await has_checksum_been_modified(resource_id, sha1)
        if checksum_modified:
            document["analysis:checksum_last_modified"] = datetime.utcnow().isoformat()

        await send(dataset_id=dataset_id,
                   resource_id=resource_id,
                   document=document)
        return {
            "checksum": sha1,
            "error": error,
            "filesize": filesize,
            "mime_type": mime_type
        }
    except IOError:
        error = "File too large to download"
        document = {
            "analysis:error": error,
            "analysis:filesize": None,
            "analysis:mime": None,
        }
        await send(dataset_id=dataset_id,
                   resource_id=resource_id,
                   document=document)
        return {
            "checksum": sha1,
            "error": error,
            "filesize": filesize,
            "mime_type": mime_type
        }
    finally:
        if tmp_file:
            os.remove(tmp_file.name)


async def has_checksum_been_modified(resource_id, new_checksum):
    q = """
    SELECT
        checksum
    FROM checks
    WHERE resource_id = $1 and checksum IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetch(q, resource_id)
        if data:
            if data[0]["checksum"] != new_checksum:
                return True
            else:
                return False
        else:
            # First check, thus we don't consider the checksum has been modified
            return False


async def compute_checksum_from_file(filename):
    # Compute sha1 in blocks
    sha1sum = hashlib.sha1()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = f.read(2**16)
    return sha1sum.hexdigest()

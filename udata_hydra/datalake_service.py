import json
import logging
import os
import tempfile

from datetime import datetime
from typing import BinaryIO

import aiohttp
import magic

from dateutil.parser import parse as date_parser, ParserError

from udata_hydra import config, context
from udata_hydra.utils.db import update_check, get_check
from udata_hydra.utils.file import compute_checksum_from_file
from udata_hydra.utils.http import send


log = logging.getLogger("udata-hydra")


async def download_resource(url: str, headers: dict) -> BinaryIO:
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
    async with aiohttp.ClientSession() as session:
        async with session.get(url, allow_redirects=True) as response:
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
async def process_resource(check_id: int) -> None:
    """
    Perform analysis on the resource designated by check_id
    - change analysis
    - size
    - mime_type
    # FIXME: saving to minio and parsing CSV not called for now
    """
    check = await get_check(check_id)
    if not check:
        log.error(f"Check not found by id {check_id}")
        return

    resource_id = check["resource_id"]
    dataset_id = check["dataset_id"]
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")

    log.debug(f"Analysis for resource {resource_id} in dataset {dataset_id}")

    # let's see if we can infer a modification date from harvest infos
    change_analysis = await detect_resource_change_from_harvest(resource_id) or {}
    # if not, let's see if we can infer a modifification date from headers
    change_analysis = change_analysis or await detect_resource_change_from_headers(url) or {}

    tmp_file = None
    sha1 = None
    error = None
    # TODO: those can be infered from header, we could skip download totally
    mime_type = None
    filesize = None

    try:
        tmp_file = await download_resource(url, headers)
    except IOError:
        error = "File too large to download"
    else:
        # Get file size
        filesize = os.path.getsize(tmp_file.name)
        # Get checksum
        sha1 = compute_checksum_from_file(tmp_file.name)
        # Check if checksum has been modified if we don't have other hints
        change_analysis = change_analysis or await detect_resource_change_from_checksum(resource_id, sha1) or {}
        # FIXME: this never seems to output text/csv, maybe override it later
        mime_type = magic.from_file(tmp_file.name, mime=True)
    finally:
        if tmp_file:
            os.remove(tmp_file.name)
        await send(
            dataset_id=dataset_id,
            resource_id=resource_id,
            document={
                "analysis:error": error,
                "analysis:filesize": filesize,
                "analysis:mime-type": mime_type,
                "analysis:checksum": sha1,
                **change_analysis,
            }
        )
        await update_check(check_id, {
            "checksum": sha1,
            "analysis_error": error,
            "filesize": filesize,
            "mime_type": mime_type
        })


async def detect_resource_change_from_checksum(resource_id, new_checksum):
    """
    Checks if resource checksum has changed over time
    Returns {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "computed-checksum",
    } or None if no match
    """
    q = """
    SELECT checksum
    FROM checks
    WHERE resource_id = $1 and checksum IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetchrow(q, resource_id)
        if data and data["checksum"] != new_checksum:
            return {
                "analysis:last-modified-at": datetime.utcnow().isoformat(),
                "analysis:last-modified-detection": "computed-checksum",
            }


async def detect_resource_change_from_headers(value: str, column: str = "url"):
    """
    Try to guess if a resource has been modified from headers in check data:
    - last-modified header value if it can be found and parsed
    - content-length if it is found and changed over time (vs last checks)

    Returns {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "detection-method",
    } or None if no guess

    TODO: this could be split two dedicated functions
    """
    # do we have a last-modified on the latest check?
    q = f"""
    SELECT
        checks.headers->>'last-modified' as last_modified,
        checks.headers->>'content-length' as content_length,
        catalog.url
    FROM checks, catalog
    WHERE checks.id = catalog.last_check
    AND catalog.{column} = $1
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetchrow(q, value)
    if not data:
        return

    # last modified header check
    if data["last_modified"]:
        try:
            # this is GMT so we should be able to safely ignore tz info
            last_modified_date = date_parser(data["last_modified"], ignoretz=True).isoformat()
            return {
                "analysis:last-modified-at": last_modified_date,
                "analysis:last-modified-detection": "last-modified-header",
            }
        except ParserError:
            pass

    # switch to content-length comparison
    if not data["content_length"]:
        return
    q = """
    SELECT
        created_at,
        checks.headers->>'content-length' as content_length
    FROM checks
    WHERE url = $1
    ORDER BY created_at DESC
    """
    async with pool.acquire() as connection:
        data = await connection.fetch(q, data["url"])
    # not enough checks to make a comparison
    if len(data) <= 1:
        return
    changed_at = None
    last_length = None
    previous_date = None
    for check in data:
        if not check["content_length"]:
            continue
        if not last_length:
            last_length = check["content_length"]
        else:
            if check["content_length"] != last_length:
                changed_at = previous_date
                break
        previous_date = check["created_at"]
    if changed_at:
        return {
            "analysis:last-modified-at": changed_at.isoformat(),
            "analysis:last-modified-detection": "content-length-header",
        }


async def detect_resource_change_from_harvest(resource_id):
    """
    Checks if resource has a harvest.modified_at
    Returns {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "harvest-resource-metadata",
    } or None if no match
    """
    q = """
        SELECT harvest_modified_at FROM catalog
        WHERE resource_id = $1
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetchrow(q, resource_id)
        if data["harvest_modified_at"]:
            return {
                "analysis:last-modified-at": data["harvest_modified_at"].isoformat(),
                "analysis:last-modified-detection": "harvest-resource-metadata",
            }

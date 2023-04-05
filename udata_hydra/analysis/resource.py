import json
import logging
import os
import pytz

from datetime import datetime
from typing import Union

import magic

from dateparser import parse as date_parser

from udata_hydra import context
from udata_hydra.utils import queue
from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.utils.csv import detect_csv_from_headers
from udata_hydra.utils.db import update_check, get_check
from udata_hydra.utils.file import compute_checksum_from_file, download_resource
from udata_hydra.utils.http import send


log = logging.getLogger("udata-hydra")


async def process_resource(check_id: int, is_first_check: bool) -> None:
    """
    Perform analysis on the resource designated by check_id
    - change analysis
    - size (optionnal)
    - mime_type (optionnal)
    - checksum (optionnal)
    - launch csv_analysis if looks like a CSV respone

    Will call udata if first check or changes found, and update check with optionnal infos
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

    # could it be a CSV? If we get hints, we will download the file
    is_csv = await detect_csv_from_headers(check)

    # if no change analysis or first time csv let's download the file to get some hints and other infos
    dl_analysis = {}
    if not change_analysis or (is_csv and is_first_check):
        tmp_file = None
        try:
            tmp_file = await download_resource(url, headers)
        except IOError:
            dl_analysis["analysis:error"] = "File too large to download"
        else:
            # Get file size
            dl_analysis["analysis:filesize"] = os.path.getsize(tmp_file.name)
            # Get checksum
            dl_analysis["analysis:checksum"] = compute_checksum_from_file(tmp_file.name)
            # Check if checksum has been modified if we don't have other hints
            change_analysis = (
                await detect_resource_change_from_checksum(resource_id, dl_analysis["analysis:checksum"])
                or {}
            )
            dl_analysis["analysis:mime-type"] = magic.from_file(tmp_file.name, mime=True)
        finally:
            if tmp_file and not is_csv:
                os.remove(tmp_file.name)
            await update_check(check_id, {
                "checksum": dl_analysis.get("analysis:checksum"),
                "analysis_error": dl_analysis.get("analysis:error"),
                "filesize": dl_analysis.get("analysis:filesize"),
                "mime_type": dl_analysis.get("analysis:mime-type"),
            })

    has_changed_over_time = await detect_has_changed_over_time(change_analysis, resource_id, check_id)

    analysis_results = {**dl_analysis, **change_analysis}
    if has_changed_over_time or (is_first_check and analysis_results):
        if is_csv and tmp_file:
            queue.enqueue(analyse_csv, check_id, file_path=tmp_file.name, _priority="default")
        queue.enqueue(
            send,
            dataset_id=dataset_id,
            resource_id=resource_id,
            document=analysis_results,
            _priority="high",
        )


async def detect_has_changed_over_time(change_analysis, resource_id, check_id) -> bool:
    """
    Determine if our detected last modified date has changed since last check
    because some methods (eg last-modified header) do not embed this
    """
    has_changed_over_time = False

    last_modified = change_analysis.get("analysis:last-modified-at")
    if not last_modified:
        return False
    last_modified = datetime.fromisoformat(last_modified)

    pool = await context.pool()

    # those detection methods already embed comparison over time, we trust them
    TRUSTED_METHODS = ["computed-checksum", "content-length-header"]
    last_modified_method = change_analysis.get("analysis:last-modified-detection")
    if last_modified_method in TRUSTED_METHODS:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE checks SET detected_last_modified_at = $1 WHERE id = $2",
                last_modified, check_id
            )
        return True

    q = """
    SELECT detected_last_modified_at
    FROM checks
    WHERE resource_id = $1 AND detected_last_modified_at IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
    """
    async with pool.acquire() as conn:
        res = await conn.fetchrow(q, resource_id)
        if res and res["detected_last_modified_at"] != last_modified:
            has_changed_over_time = True
        # keep date in store for next run
        await conn.execute(
            "UPDATE checks SET detected_last_modified_at = $1 WHERE id = $2",
            last_modified, check_id
        )

    return has_changed_over_time


async def detect_resource_change_from_checksum(resource_id, new_checksum) -> Union[dict, None]:
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
                "analysis:last-modified-at": datetime.now(pytz.UTC).isoformat(),
                "analysis:last-modified-detection": "computed-checksum",
            }


async def detect_resource_change_from_headers(value: str, column: str = "url") -> Union[dict, None]:
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
        last_modified_date = date_parser(data["last_modified"])
        if last_modified_date:
            return {
                "analysis:last-modified-at": last_modified_date.isoformat(),
                "analysis:last-modified-detection": "last-modified-header",
            }

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


async def detect_resource_change_from_harvest(resource_id) -> Union[dict, None]:
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

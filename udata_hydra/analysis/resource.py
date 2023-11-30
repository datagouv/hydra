from enum import Enum
import json
import logging
import os
import pytz

from datetime import datetime
from typing import Tuple, Union

import magic

from dateparser import parse as date_parser

from udata_hydra import context, config
from udata_hydra.utils import queue
from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.utils.csv import detect_csv_from_headers
from udata_hydra.utils.db import update_check, get_check
from udata_hydra.utils.file import compute_checksum_from_file, download_resource
from udata_hydra.utils.http import send


class Change(Enum):
    HAS_CHANGED = 1
    HAS_NOT_CHANGED = 2
    NO_GUESS = 3


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

    exceptions = config.LARGE_RESOURCES_EXCEPTIONS

    resource_id = check["resource_id"]
    dataset_id = check["dataset_id"]
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")

    log.debug(f"Analysis for resource {resource_id} in dataset {dataset_id}")

    # let's see if we can infer a modification date on early hints based on harvest infos and headers
    change_status, change_payload = await detect_resource_change_on_early_hints(resource_id)

    # could it be a CSV? If we get hints, we will analyse the file further depending on change status
    is_csv = await detect_csv_from_headers(check)

    # if the change status is NO_GUESS or HAS_CHANGED, let's download the file to get more infos
    dl_analysis = {}
    tmp_file = None
    if change_status != Change.HAS_NOT_CHANGED:
        try:
            tmp_file = await download_resource(url, headers, str(resource_id) in exceptions)
        except IOError:
            dl_analysis["analysis:error"] = "File too large to download"
        else:
            # Get file size
            dl_analysis["analysis:content-length"] = os.path.getsize(tmp_file.name)
            # Get checksum
            dl_analysis["analysis:checksum"] = compute_checksum_from_file(tmp_file.name)
            # Check if checksum has been modified if we don't have other hints
            if change_status == Change.NO_GUESS:
                change_status,  change_payload = await detect_resource_change_from_checksum(
                    resource_id, dl_analysis["analysis:checksum"])
            dl_analysis["analysis:mime-type"] = magic.from_file(tmp_file.name, mime=True)
        finally:
            if tmp_file and not is_csv:
                os.remove(tmp_file.name)
            await update_check(check_id, {
                "checksum": dl_analysis.get("analysis:checksum"),
                "analysis_error": dl_analysis.get("analysis:error"),
                "filesize": dl_analysis.get("analysis:content-length"),
                "mime_type": dl_analysis.get("analysis:mime-type"),
            })

    if change_status == Change.HAS_CHANGED:
        await store_last_modified_date(change_payload or {}, resource_id, check_id)

    analysis_results = {**dl_analysis, **(change_payload or {})}
    if change_status == Change.HAS_CHANGED or is_first_check:
        if is_csv and tmp_file:
            queue.enqueue(analyse_csv, check_id, file_path=tmp_file.name, _priority="default")
        queue.enqueue(
            send,
            dataset_id=dataset_id,
            resource_id=resource_id,
            document=analysis_results,
            _priority="high",
        )


async def store_last_modified_date(change_analysis, resource_id, check_id) -> None:
    """
    Store last modified date in checks because it may be useful for later comparison
    """
    pool = await context.pool()
    last_modified = change_analysis.get("analysis:last-modified-at")
    if last_modified:
        last_modified = datetime.fromisoformat(last_modified)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE checks SET detected_last_modified_at = $1 WHERE id = $2",
                last_modified, check_id
            )


async def detect_resource_change_from_checksum(resource_id, new_checksum) -> Tuple[Change, Union[dict, None]]:
    """
    Checks if resource checksum has changed over time
    Returns a tuple with a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "computed-checksum",
    }
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
            return Change.HAS_CHANGED, {
                "analysis:last-modified-at": datetime.now(pytz.UTC).isoformat(),
                "analysis:last-modified-detection": "computed-checksum",
            }
    return Change.NO_GUESS, None


async def detect_resource_change_from_last_modified_header(data: dict) -> Tuple[Change, Union[dict, None]]:
    # last modified header check

    if len(data) == 1 and data[0]["last_modified"]:
        last_modified_date = date_parser(data[0]["last_modified"])
        return Change.HAS_CHANGED, {
            "analysis:last-modified-at": last_modified_date.isoformat(),
            "analysis:last-modified-detection": "last-modified-header",
        }

    if len(data) == 1 or not data[0]["last_modified"]:
        return Change.NO_GUESS, None

    if data[0]["last_modified"] != data[1]["last_modified"]:
        last_modified_date = date_parser(data[0]["last_modified"])
        return Change.HAS_CHANGED, {
            "analysis:last-modified-at": last_modified_date.isoformat(),
            "analysis:last-modified-detection": "last-modified-header",
        }
    return Change.HAS_NOT_CHANGED, None


async def detect_resource_change_from_content_length_header(data: dict) -> Tuple[Change, Union[dict, None]]:
    # content-length variation between current and last check
    if len(data) <= 1 or not data[0]["content_length"]:
        return Change.NO_GUESS, None
    if data[0]["content_length"] != data[1]["content_length"]:
        changed_at = data[0]["created_at"]
        return Change.HAS_CHANGED, {
            "analysis:last-modified-at": changed_at.isoformat(),
            "analysis:last-modified-detection": "content-length-header",
        }
    return Change.HAS_NOT_CHANGED, None


async def detect_resource_change_on_early_hints(resource_id: str) -> Tuple[Change, Union[dict, None]]:
    """
    Try to guess if a resource has been modified from harvest and headers in check data:
    - last-modified header value if it can be found and parsed
    - content-length if it is found and changed over time (vs last checks)

    Returns a tuple with a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "detection-method",
    }
    """
    # Fetch current and last check headers
    q = """
    SELECT
        created_at,
        headers->>'last-modified' as last_modified,
        headers->>'content-length' as content_length,
        detected_last_modified_at
    FROM checks
    WHERE resource_id = $1
    ORDER BY created_at DESC
    LIMIT 2
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetch(q, resource_id)

    # not enough checks to make a comparison
    if not data:
        return Change.NO_GUESS, None

    # let's see if we can infer a modification date from harvest infos
    change_status, change_payload = await detect_resource_change_from_harvest(data, resource_id)
    if change_status != Change.NO_GUESS:
        return change_status, change_payload

    # if not, let's see if we can infer a modifification date from last-modified headers
    change_status, change_payload = await detect_resource_change_from_last_modified_header(data)
    if change_status != Change.NO_GUESS:
        return change_status, change_payload

    # if not, let's see if we can infer a modifification date from content-length header
    return await detect_resource_change_from_content_length_header(data)


async def detect_resource_change_from_harvest(checks_data: dict, resource_id: str) -> Tuple[Change, Union[dict, None]]:
    """
    Checks if resource has a harvest.modified_at
    Returns a tuple with a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "harvest-resource-metadata",
    }
    """
    if len(checks_data) == 1:
        return Change.NO_GUESS, None
    last_check = checks_data[1]
    q = """
        SELECT catalog.harvest_modified_at FROM catalog
        WHERE catalog.resource_id = $1
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        catalog_data = await connection.fetchrow(q, resource_id)
        if catalog_data and catalog_data["harvest_modified_at"]:
            if catalog_data["harvest_modified_at"] == last_check["detected_last_modified_at"]:
                return Change.HAS_NOT_CHANGED, None
            return Change.HAS_CHANGED, {
                "analysis:last-modified-at": catalog_data["harvest_modified_at"].isoformat(),
                "analysis:last-modified-detection": "harvest-resource-metadata",
            }
    return Change.NO_GUESS, None

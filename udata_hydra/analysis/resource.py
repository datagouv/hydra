from enum import Enum
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

    resource_id = check["resource_id"]
    dataset_id = check["dataset_id"]
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")

    log.debug(f"Analysis for resource {resource_id} in dataset {dataset_id}")

    # let's see if we can infer a modification date from harvest infos
    change_status, change_payload = await detect_resource_change_from_harvest(resource_id) or {}
    # if not, let's see if we can infer a modifification date from headers
    if change_status == Change.NO_GUESS:
        change_status,  change_payload = await detect_resource_change_from_headers(resource_id, column="resource_id")

    # could it be a CSV? If we get hints, we will download the file
    is_csv = await detect_csv_from_headers(check)


    # Compare previous header on last-modification header to know if it hasn't changed
    # and if we should download to do more analysis

    # if no change analysis or first time csv let's download the file to get some hints and other infos

    # If we have detected a change, we want to download the file to

    dl_analysis = {}
    tmp_file = None
    if change_status != Change.HAS_NOT_CHANGED:
        # We should enter this condition if change_analysis is None or an object, but not False
        try:
            tmp_file = await download_resource(url, headers)
        except IOError:
            dl_analysis["analysis:error"] = "File too large to download"
        else:
            # Get file size
            dl_analysis["analysis:content-length"] = os.path.getsize(tmp_file.name)
            # Get checksum
            dl_analysis["analysis:checksum"] = compute_checksum_from_file(tmp_file.name)
            # Check if checksum has been modified if we don't have other hints
            if change_status == Change.NO_GUESS:
                change_status,  change_payload = await detect_resource_change_from_checksum(resource_id, dl_analysis["analysis:checksum"])
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


async def store_last_modified_date(change_analysis, resource_id, check_id) -> bool:
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


async def detect_resource_change_from_checksum(resource_id, new_checksum) -> Union[dict, None]:
    """
    Checks if resource checksum has changed over time
    Returns a Change status and an optional payload:
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


async def detect_resource_change_from_last_modified_header(data: dict):
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


async def detect_resource_change_from_content_length_header(data: dict):
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


async def detect_resource_change_from_headers(value: str, column: str = "url") -> Union[dict, None]:
    """
    Try to guess if a resource has been modified from headers in check data:
    - last-modified header value if it can be found and parsed
    - content-length if it is found and changed over time (vs last checks)

    Returns a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "detection-method",
    }


    """
    # Fetch current and last check headers
    q = f"""
    SELECT
        created_at,
        checks.headers->>'last-modified' as last_modified,
        checks.headers->>'content-length' as content_length
    FROM checks, catalog
    WHERE checks.{column} = $1
    ORDER BY created_at DESC
    LIMIT 2
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetch(q, value)

    # not enough checks to make a comparison
    if not data:
        return Change.NO_GUESS, None

    change_status, change_payload = await detect_resource_change_from_last_modified_header(data)
    if change_status != Change.NO_GUESS:
        return change_status, change_payload
    return await detect_resource_change_from_content_length_header(data)


async def detect_resource_change_from_harvest(resource_id) -> Union[dict, None]:
    """
    Checks if resource has a harvest.modified_at
    Returns a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "harvest-resource-metadata",
    }
    """
    q = """
        SELECT catalog.harvest_modified_at, checks.detected_last_modified_at FROM catalog, checks
        WHERE catalog.resource_id = $1
        AND checks.id = catalog.last_check
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        data = await connection.fetchrow(q, resource_id)
        if data and data["harvest_modified_at"]:
            if data["harvest_modified_at"] == data["detected_last_modified_at"]:
                return Change.HAS_NOT_CHANGED, None
            return Change.HAS_CHANGED, {
                "analysis:last-modified-at": data["harvest_modified_at"].isoformat(),
                "analysis:last-modified-detection": "harvest-resource-metadata",
            }
    return Change.NO_GUESS, None

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum

import magic
from asyncpg import Record
from dateparser import parse as date_parser

from udata_hydra import config, context
from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.analysis.geojson import analyse_geojson
from udata_hydra.crawl.calculate_next_check import calculate_next_check_date
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    Timer,
    UdataPayload,
    compute_checksum_from_file,
    detect_geojson_from_headers_or_catalog,
    detect_tabular_from_headers,
    download_resource,
    queue,
    send,
)


class Change(Enum):
    HAS_CHANGED = 1
    HAS_NOT_CHANGED = 2
    NO_GUESS = 3


log = logging.getLogger("udata-hydra")


async def analyse_resource(
    check: dict,
    last_check: dict | None,
    force_analysis: bool = False,
    worker_priority: str = "default",
) -> None:
    """
    Perform analysis on the resource designated by check_id:
    - change analysis
    - size (optional)
    - mime_type (optional)
    - checksum (optional)
    - launch csv_analysis if looks like a CSV response

    Will call udata if first check or changes found, and update check with optional infos
    """

    # Check if the resource is in the exceptions table
    exception: Record | None = await ResourceException.get_by_resource_id(str(check["resource_id"]))

    resource_id = check["resource_id"]
    dataset_id = check["dataset_id"]
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")

    timer = Timer("analyse-resource", resource_id)

    log.debug(f"Analysis for resource {resource_id} in dataset {dataset_id}")

    # Update resource status to ANALYSING_RESOURCE
    resource: Record | None = await Resource.update(
        resource_id, data={"status": "ANALYSING_RESOURCE_HEAD"}
    )

    # let's see if we can infer a modification date on early hints based on harvest infos and headers
    change_status, change_payload = await detect_resource_change_on_early_hints(resource)

    # could it be a CSV or a GeoJSON? If we get hints, we will analyse the file further depending on change status
    is_tabular, file_format = detect_tabular_from_headers(check)
    is_geojson: bool = await detect_geojson_from_headers_or_catalog(check)
    if is_geojson:
        file_format = "geojson"
    max_size_allowed = None if exception else int(config.MAX_FILESIZE_ALLOWED[file_format])

    # if the change status is NO_GUESS or HAS_CHANGED, let's download the file to get more infos
    dl_analysis = {}
    tmp_file = None
    if change_status != Change.HAS_NOT_CHANGED or force_analysis:
        try:
            await Resource.update(resource_id, data={"status": "DOWNLOADING_RESOURCE"})
            tmp_file, _ = await download_resource(url, headers, max_size_allowed)
            timer.mark("download-resource")
        except IOException:
            dl_analysis["analysis:error"] = "File too large to download"
        else:
            await Resource.update(resource_id, data={"status": "ANALYSING_DOWNLOADED_RESOURCE"})
            # Get file size
            dl_analysis["analysis:content-length"] = os.path.getsize(tmp_file.name)
            # Get checksum
            dl_analysis["analysis:checksum"] = compute_checksum_from_file(tmp_file.name)
            # Check if checksum has been modified if we don't have other hints
            if change_status == Change.NO_GUESS:
                (
                    change_status,
                    change_payload,
                ) = await detect_resource_change_from_checksum(
                    new_checksum=dl_analysis["analysis:checksum"], last_check=last_check
                )
            dl_analysis["analysis:mime-type"] = magic.from_file(tmp_file.name, mime=True)
        finally:
            if tmp_file and not (is_tabular or is_geojson):
                os.remove(tmp_file.name)
            await Check.update(
                check["id"],
                {
                    "checksum": dl_analysis.get("analysis:checksum"),
                    "analysis_error": dl_analysis.get("analysis:error"),
                    "filesize": dl_analysis.get("analysis:content-length"),
                    "mime_type": dl_analysis.get("analysis:mime-type"),
                },
            )

    # if the file has not changed, update the current check with the infos of the previous one
    elif last_check:
        await Check.update(
            check["id"],
            {k: last_check.get(k) for k in ["checksum", "analysis_error", "filesize", "mime_type"]},
        )

    if change_status in (Change.HAS_CHANGED, Change.HAS_NOT_CHANGED):
        await update_check_with_modification_and_next_dates(
            change_analysis=change_payload or {},
            check_id=check["id"],
            last_check=last_check,
            has_changed=change_status == Change.HAS_CHANGED,
        )

    analysis_results = {"analysis:check_id": check["id"]} | dl_analysis | (change_payload or {})

    timer.stop()

    if change_status == Change.HAS_CHANGED or not last_check or force_analysis:
        if is_tabular and tmp_file:
            # Change status to TO_ANALYSE_CSV
            await Resource.update(resource_id, data={"status": "TO_ANALYSE_CSV"})
            # Analyse CSV and create a table in the CSV database
            queue.enqueue(
                analyse_csv,
                check=check,
                file_path=tmp_file.name,
                _priority="high" if worker_priority == "high" else "default",
                _exception=bool(exception),
            )
        elif is_geojson and tmp_file:
            await Resource.update(resource_id, data={"status": "TO_ANALYSE_GEOJSON"})
            queue.enqueue(
                analyse_geojson,
                check=check,
                file_path=tmp_file.name,
                _priority="high" if worker_priority == "high" else "default",
                _exception=bool(exception),
            )
        else:
            await Resource.update(resource_id, data={"status": None})

        # Send analysis result to udata
        queue.enqueue(
            send,
            dataset_id=dataset_id,
            resource_id=resource_id,
            document=UdataPayload(analysis_results),
            _priority="high",
        )

    else:
        await Resource.update(resource_id, data={"status": None})


async def update_check_with_modification_and_next_dates(
    change_analysis: dict, check_id: int, last_check: dict | None, has_changed: bool = False
) -> None:
    """Update check with last_modified date and next_check date only if resource has changed

    Args:
        change_analysis: dict with optional "analysis:last-modified-at" key
        check_id: the ID of the current check
        last_check: the last check data, if any
        has_changed: if the resource has changed since last check
    """
    last_modified: str | None = change_analysis.get("analysis:last-modified-at")
    if last_modified:
        last_modified_at: datetime = datetime.fromisoformat(last_modified)
        payload = {"detected_last_modified_at": last_modified_at}
        if has_changed:
            next_check_at: datetime = calculate_next_check_date(
                has_check_changed=True, last_check=last_check, last_modified_at=last_modified_at
            )
            payload["next_check_at"] = next_check_at
        await Check.update(check_id, payload)


async def detect_resource_change_from_checksum(
    new_checksum, last_check: dict | None
) -> tuple[Change, dict | None]:
    """
    Checks if resource checksum has changed over time
    Returns a tuple with a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "computed-checksum",
    }
    """
    if last_check and last_check.get("checksum"):
        if last_check.get("checksum") != new_checksum:
            return Change.HAS_CHANGED, {
                "analysis:last-modified-at": datetime.now(timezone.utc).isoformat(),
                "analysis:last-modified-detection": "computed-checksum",
            }
        elif last_check.get("detected_last_modified_at"):
            return Change.HAS_NOT_CHANGED, {
                "analysis:last-modified-at": last_check["detected_last_modified_at"].isoformat(),
                "analysis:last-modified-detection": "previous-check-detection",
            }
    # if the previous check did not have the info, we investigate further
    return Change.NO_GUESS, None


async def detect_resource_change_from_last_modified_header(
    data: dict,
) -> tuple[Change, dict | None]:
    # last modified header check

    if len(data) == 1 and data[0]["last_modified"]:
        last_modified_date = date_parser(data[0]["last_modified"])
        return Change.HAS_CHANGED, {
            "analysis:last-modified-at": last_modified_date.isoformat(),
            "analysis:last-modified-detection": "last-modified-header",
        }

    if len(data) == 1 or not data[0]["last_modified"]:
        return Change.NO_GUESS, None

    last_modified_date = date_parser(data[0]["last_modified"])
    payload = {
        "analysis:last-modified-at": last_modified_date.isoformat(),
        "analysis:last-modified-detection": "last-modified-header",
    }
    if data[0]["last_modified"] != data[1]["last_modified"]:
        return Change.HAS_CHANGED, payload
    return Change.HAS_NOT_CHANGED, payload


async def detect_resource_change_from_content_length_header(
    data: dict,
) -> tuple[Change, dict | None]:
    # content-length variation between current and last check
    if len(data) <= 1 or not data[0].get("content_length"):
        return Change.NO_GUESS, None
    if data[0].get("content_length") and data[1].get("content_length"):
        if data[0]["content_length"] != data[1]["content_length"]:
            return Change.HAS_CHANGED, {
                # if resource has changed, set last-modified to the current check's creation
                "analysis:last-modified-at": data[0]["created_at"].isoformat(),
                "analysis:last-modified-detection": "content-length-header",
            }
        # same content_length is not 100% certainly no change, but a good tradeoff to prevent many downloads
        elif data[1].get("detected_last_modified_at"):
            return Change.HAS_NOT_CHANGED, {
                # no change, using the last-modified from the previous check (passed on from check to check)
                "analysis:last-modified-at": data[1]["detected_last_modified_at"].isoformat(),
                "analysis:last-modified-detection": "previous-check-detection",
            }
    # if the previous check did not have the info, we investigate further
    return Change.NO_GUESS, None


async def detect_resource_change_on_early_hints(
    resource: Record | None,
) -> tuple[Change, dict | None]:
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
    data = None

    if resource:
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
            data = await connection.fetch(q, str(resource["resource_id"]))

    # not enough checks to make a comparison
    if not data:
        return Change.NO_GUESS, None

    # let's see if we can infer a modification date from harvest infos
    change_status, change_payload = await detect_resource_change_from_harvest(data, resource)
    if change_status != Change.NO_GUESS:
        return change_status, change_payload

    # if not, let's see if we can infer a modifification date from last-modified headers
    (
        change_status,
        change_payload,
    ) = await detect_resource_change_from_last_modified_header(data)
    if change_status != Change.NO_GUESS:
        return change_status, change_payload

    # if not, let's see if we can infer a modifification date from content-length header
    return await detect_resource_change_from_content_length_header(data)


async def detect_resource_change_from_harvest(
    checks_data: tuple, resource: Record | None
) -> tuple[Change, dict | None]:
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

    if resource and resource.get("harvest_modified_at"):
        payload = {
            "analysis:last-modified-at": resource["harvest_modified_at"].isoformat(),
            "analysis:last-modified-detection": "harvest-resource-metadata",
        }
        if resource["harvest_modified_at"] == last_check["detected_last_modified_at"]:
            return Change.HAS_NOT_CHANGED, payload
        return Change.HAS_CHANGED, payload
    return Change.NO_GUESS, None

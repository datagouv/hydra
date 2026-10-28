import json
import logging
import os
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path

import magic
from asyncpg import Record
from dateparser import parse as date_parser

from udata_hydra import config, context
from udata_hydra.crawl.calculate_next_check import calculate_next_check_date
from udata_hydra.data_formats import (
    DataFormat,
    Gz,
    Ogc,
)
from udata_hydra.data_formats.detect import detect_data_format_from_check_or_catalog
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    Timer,
    UdataPayload,
    compute_checksum_from_file,
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

    Will call udata if first analysis, new URL, changes found or forced analysis,
    and update check with optional infos
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

    # compare with the last analysed check, not catalog.last_check: checks that failed in between
    # (timeout, HTTP error) were never downloaded and tell nothing about the file
    last_check: Record | None = await Check.get_previous_analysed(str(resource_id), check["id"])

    # the parsing table is named after the URL: a URL never analysed has no table yet
    must_analyse: bool = force_analysis or not last_check or last_check["url"] != url

    # let's see if we can infer a modification date on early hints based on harvest infos and headers
    change_status, change_payload = await detect_resource_change_on_early_hints(
        resource, check_id=check["id"], last_check_id=last_check["id"] if last_check else None
    )

    # could it be a CSV, parquet, GeoJSON or OGC service (WFS)?
    # If we get hints, we will analyse the file further depending on change status
    data_format: type[DataFormat] | None = await detect_data_format_from_check_or_catalog(check)

    max_size_allowed = (
        None
        if exception
        else data_format.max_filesize_allowed
        if data_format is not None
        else config.DEFAULT_MAX_FILESIZE_ALLOWED
    )

    dl_analysis = {}
    tmp_file = None
    analysis_file_name = None
    if change_status != Change.HAS_NOT_CHANGED or must_analyse:
        try:
            await Resource.update(resource_id, data={"status": "DOWNLOADING_RESOURCE"})
            tmp_file, _ = await download_resource(url, headers, max_size_allowed)
            timer.mark("download-resource")
            analysis_path = Path(tmp_file.name)
            analysis_file_name = analysis_path.name
            if data_format is Gz:
                gz_file = Gz(
                    file_name=analysis_file_name,
                    resource_id=str(resource_id),
                    dataset_id=dataset_id,
                )
                gz_file.unwrap()
                analysis_file_name = gz_file.file_name
                analysis_path = gz_file.path
        except IOException as e:
            # FileTooLargeException.report_to_sentry is False; other IO errors are reported.
            dl_analysis["analysis:error"] = str(e)
        else:
            await Resource.update(resource_id, data={"status": "ANALYSING_DOWNLOADED_RESOURCE"})
            # Get file size
            dl_analysis["analysis:content-length"] = analysis_path.stat().st_size
            # Get checksum
            dl_analysis["analysis:checksum"] = compute_checksum_from_file(str(analysis_path))
            # Check if checksum has been modified if we don't have other hints
            if change_status == Change.NO_GUESS:
                (
                    change_status,
                    change_payload,
                ) = await detect_resource_change_from_checksum(
                    new_checksum=dl_analysis["analysis:checksum"], last_check=last_check
                )
            # Inner payload MIME after Gz.unwrap when applicable; catalog keeps the declared type.
            dl_analysis["analysis:mime-type"] = magic.from_file(str(analysis_path), mime=True)
        finally:
            if tmp_file and (data_format is None or not data_format.further_analysis):
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

    has_changed: bool = change_status == Change.HAS_CHANGED or must_analyse

    if has_changed:
        # Send analysis result to udata
        queue.enqueue(
            send,
            dataset_id=dataset_id,
            resource_id=resource_id,
            document=UdataPayload(analysis_results),
            _priority="high",
        )
    else:
        log.info(
            f"[resource_id={resource_id}] analyse_resource: skipped analysis udata notify "
            f"(change_status={change_status.name}, last_parsing_table="
            f"{last_check.get('parsing_table') if last_check else None})"
        )

    ogc_format: type[Ogc] | None = (
        data_format if data_format is not None and issubclass(data_format, Ogc) else None
    )

    # OGC services are analysed on their own schedule: the GetCapabilities document can stay
    # the same for years or change on every check, whatever happens to the service
    if ogc_format is not None:
        # a failed download (e.g. too large) rules out the analysis, which would fetch the document again
        if not dl_analysis.get("analysis:error") and await is_ogc_analysis_due(
            check, must_analyse=must_analyse
        ):
            log.info(
                f"[resource_id={resource_id}] analyse_resource: enqueued {ogc_format.__name__} analysis"
            )
            await Resource.update(
                resource_id, data={"status": f"TO_ANALYSE_{ogc_format.__name__.upper()}"}
            )
            queue.enqueue(
                ogc_format.analyse,
                check=dict(check),
                _priority="high" if worker_priority == "high" else "default",
                _exception=bool(exception),
            )
        else:
            await Resource.update(resource_id, data={"status": None})
        return

    if has_changed and data_format is not None and not dl_analysis.get("analysis:error"):
        log.info(
            f"[resource_id={resource_id}] analyse_resource: enqueued {data_format.__name__} analysis"
        )
        await Resource.update(
            resource_id, data={"status": f"TO_ANALYSE_{data_format.__name__.upper()}"}
        )
        if tmp_file and analysis_file_name:
            file = data_format(
                file_name=analysis_file_name,
                resource_id=resource_id,
                dataset_id=dataset_id,
            )
            queue.enqueue(
                file.analyse,
                check=dict(check),
                _priority="high" if worker_priority == "high" else "default",
                _exception=bool(exception),
            )
    else:
        await Resource.update(resource_id, data={"status": None})


async def is_ogc_analysis_due(check: dict, must_analyse: bool) -> bool:
    """Decide whether an OGC service should be analysed, independently of the change detection:
    when must_analyse, when it never succeeded, or when the check of the last successful analysis
    is older than OGC_ANALYSIS_MAX_AGE_DAYS.
    """
    if must_analyse:
        return True

    last_ogc_analysis: Record | None = await Check.get_previous_ogc_analysed(
        str(check["resource_id"]), check["id"]
    )
    if not last_ogc_analysis:
        return True

    max_age = timedelta(days=config.OGC_ANALYSIS_MAX_AGE_DAYS)
    return last_ogc_analysis["created_at"] < datetime.now(timezone.utc) - max_age


async def update_check_with_modification_and_next_dates(
    change_analysis: dict,
    check_id: int,
    last_check: Record | dict | None,
    has_changed: bool = False,
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
    new_checksum, last_check: Record | dict | None
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
    data: list,
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
    data: list,
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
    check_id: int,
    last_check_id: int | None,
) -> tuple[Change, dict | None]:
    """
    Try to guess if a resource has been modified from harvest and headers in check data:
    - last-modified header value if it can be found and parsed
    - content-length if it is found and changed over time (vs the last analysed check)

    Returns a tuple with a Change status and an optional payload:
    {
        "analysis:last-modified-at": last_modified_date,
        "analysis:last-modified-detection": "detection-method",
    }
    """
    data = None

    if resource:
        # Fetch headers for the current check and the last analysed one
        q = """
        SELECT
            created_at,
            headers->>'last-modified' as last_modified,
            headers->>'content-length' as content_length,
            detected_last_modified_at
        FROM checks
        WHERE id = ANY($1::int[])
        ORDER BY created_at DESC
        """
        pool = await context.pool()
        async with pool.acquire() as connection:
            data = await connection.fetch(
                q, [check_id] + ([last_check_id] if last_check_id else [])
            )

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

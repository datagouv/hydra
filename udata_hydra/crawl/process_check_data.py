import json
from datetime import datetime, timedelta, timezone

from asyncpg import Record

from udata_hydra import config
from udata_hydra.crawl.helpers import get_content_type_from_header, is_valid_status
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import queue, send


async def process_check_data(check_data: dict) -> tuple[Record, bool]:
    """Preprocess a check before saving it
    Return the check and a boolean indicating if it's the first check for this resource"""

    check_data["resource_id"] = str(check_data["resource_id"])

    last_check: Record | None = await Check.get_by_resource_id(check_data["resource_id"])

    has_changed: bool = await has_check_changed(check_data, last_check)
    if has_changed:
        await send_check_to_udata(check_data)

    # Update resource following check:
    # Reset resource status so that it's not forbidden to be checked again.
    # Reset priority so that it's not prioritised anymore.
    await Resource.update(
        resource_id=check_data["resource_id"], data={"status": None, "priority": False}
    )

    is_first_check: bool = last_check is None

    # Calculate next check date
    if is_first_check:
        next_check = datetime.now(timezone.utc) + timedelta(hours=config.CHECK_DELAY_DEFAULT)

    else:
        if has_check_changed:
            # Resource has been modified since last check, we will check it again in in the soonest of the CHECK_DELAYS
            next_check = datetime.now(timezone.utc) + timedelta(
                hours=config.CHECK_DELAYS[0]
            )  # TODO: should this be CHECK_DELAY_DEFAULT?
        else:
            # Resource has not been modified since last check:
            # if the time since last check is greater than the longest delay in CHECK_DELAYS, next check will be after the longest delay
            # if the time since last check is less than CHECK_DELAYS[i], next check will be after this CHECK_DELAYS[i]
            previous_delay: timedelta = datetime.now(timezone.utc) - datetime.fromisoformat(
                last_check["created_at"]
            )
            if previous_delay > timedelta(hours=config.CHECK_DELAYS[-1]):
                next_check = datetime.now(timezone.utc) + timedelta(hours=config.CHECK_DELAYS[-1])
            else:
                for d in config.CHECK_DELAYS:
                    if previous_delay <= timedelta(hours=d):
                        next_check = datetime.now(timezone.utc) + timedelta(hours=d)
                        break
        check_data["next_check"] = next_check

    return await Check.insert(check_data), is_first_check


async def has_check_changed(check_data: dict, last_check: dict | None) -> bool:
    """Check if the check has changed compared to the last one"""

    is_first_check: bool = last_check is None
    status_has_changed = last_check and check_data.get("status") != last_check.get("status")
    status_no_longer_available = (
        last_check
        and is_valid_status(last_check.get("status"))
        and not is_valid_status(check_data.get("status"))
    )
    timeout_has_changed = last_check and check_data.get("timeout") != last_check.get("timeout")
    current_headers = check_data.get("headers", {})
    last_check_headers = (
        json.loads(last_check.get("headers")) if last_check and last_check.get("headers") else {}
    )
    content_has_changed = last_check and (
        current_headers.get("content-length") != last_check_headers.get("content-length")
        or current_headers.get("content-type") != last_check_headers.get("content-type")
    )

    # TODO: Instead of computing criterions here, store payload and compare with previous one.
    # It would make debugging easier.
    criterions = {
        "is_first_check": is_first_check,
        "status_has_changed": status_has_changed,
        "status_no_longer_available": status_no_longer_available,
        "timeout_has_changed": timeout_has_changed,
        "content_has_changed": content_has_changed,
    }

    return any(criterions.values())


async def send_check_to_udata(check_data: dict) -> None:
    """Enqueue the sending of the check to udata"""

    res = await Resource.get(resource_id=check_data["resource_id"], column_name="dataset_id")

    queue.enqueue(
        send,
        dataset_id=res["dataset_id"],
        resource_id=check_data["resource_id"],
        document={
            "check:available": is_valid_status(check_data.get("status")),
            "check:status": check_data.get("status"),
            "check:timeout": check_data["timeout"],
            "check:date": datetime.now(timezone.utc).isoformat(),
            "check:error": check_data.get("error"),
            "check:headers:content-type": await get_content_type_from_header(
                check_data.get("headers", {})
            ),
            "check:headers:content-length": int(
                check_data.get("headers", {}).get("content-length", 0)
            )
            or None,
        },
        _priority="high",
    )

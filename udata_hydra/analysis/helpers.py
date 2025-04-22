import json
from datetime import date, datetime
from typing import IO

from asyncpg import Record
from dateparser import parse as date_parser
from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parser

from udata_hydra import config
from udata_hydra.utils import UdataPayload, download_resource, queue, send


def to_json(value: str) -> str:
    """Convenience method, should be casted from string directly by postgres"""
    return value


def _parse_dt(value: str) -> datetime | None:
    """For performance reasons, we try first with dateutil and fallback on dateparser"""
    try:
        return dateutil_parser(value)
    except ParserError:
        return date_parser(value)


def to_date(value: str) -> date | None:
    parsed = _parse_dt(value)
    return parsed.date() if parsed else None


def to_datetime(value: str) -> datetime | None:
    return _parse_dt(value)


async def read_or_download_file(
    check: dict,
    file_path: str,
    file_format: str,
    exception: Record | None,
) -> IO[bytes]:
    return (
        open(file_path, "rb")
        if file_path
        else await download_resource(
            url=check["url"],
            headers=json.loads(check.get("headers") or "{}"),
            max_size_allowed=None
            if exception
            else int(config.MAX_FILESIZE_ALLOWED.get(file_format, "csv")),
        )
    )


async def notify_udata(resource: Record, check: dict) -> None:
    """Notify udata of the result of a parsing"""
    payload = {
        "resource_id": check["resource_id"],
        "dataset_id": resource["dataset_id"],
        "document": {
            "analysis:parsing:error": check["parsing_error"],
            "analysis:parsing:started_at": check["parsing_started_at"].isoformat()
            if check["parsing_started_at"]
            else None,
            "analysis:parsing:finished_at": check["parsing_finished_at"].isoformat()
            if check["parsing_finished_at"]
            else None,
        },
    }
    if config.CSV_TO_PARQUET and check.get("parquet_url"):
        payload["document"]["analysis:parsing:parquet_url"] = check.get("parquet_url")
        payload["document"]["analysis:parsing:parquet_size"] = check.get("parquet_size")
    if config.GEOJSON_TO_PMTILES and check.get("pmtiles_url"):
        payload["document"]["analysis:parsing:pmtiles_url"] = check.get("pmtiles_url")
        payload["document"]["analysis:parsing:pmtiles_size"] = check.get("pmtiles_size")
    payload["document"] = UdataPayload(payload["document"])
    queue.enqueue(send, _priority="high", **payload)

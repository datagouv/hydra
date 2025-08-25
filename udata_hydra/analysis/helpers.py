import json
from typing import IO

from asyncpg import Record

from udata_hydra import config
from udata_hydra.utils import UdataPayload, download_resource, queue, send


def get_python_type(column: dict) -> str:
    """Outsourcing the distinction of aware datetimes"""
    return (
        "datetime_aware"
        if column["format"] in {"datetime_aware", "datetime_rfc822"}
        else column["python_type"]
    )


async def read_or_download_file(
    check: dict,
    file_path: str,
    file_format: str,
    exception: Record | None,
) -> IO[bytes]:
    if file_path:
        return open(file_path, "rb")
    else:
        tmp_file, _ = await download_resource(
            url=check["url"],
            headers=json.loads(check.get("headers") or "{}"),
            max_size_allowed=None
            if exception
            else int(config.MAX_FILESIZE_ALLOWED.get(file_format, "csv")),
        )
        return tmp_file


async def notify_udata(resource: Record, check: dict) -> None:
    """Notify udata of the result of a parsing"""
    payload = {
        "resource_id": check["resource_id"],
        "dataset_id": resource["dataset_id"],
        "document": {
            "analysis:parsing:error": check.get("parsing_error"),
            "analysis:parsing:started_at": check["parsing_started_at"].isoformat()
            if check.get("parsing_started_at")
            else None,
            "analysis:parsing:finished_at": check["parsing_finished_at"].isoformat()
            if check.get("parsing_finished_at")
            else None,
        },
    }
    if config.CSV_TO_DB and check.get("parsing_table"):
        payload["document"]["analysis:parsing:parsing_table"] = check.get("parsing_table")
    if config.CSV_TO_PARQUET and check.get("parquet_url"):
        payload["document"]["analysis:parsing:parquet_url"] = check.get("parquet_url")
        payload["document"]["analysis:parsing:parquet_size"] = check.get("parquet_size")
    if config.GEOJSON_TO_PMTILES and check.get("pmtiles_url"):
        payload["document"]["analysis:parsing:pmtiles_url"] = check.get("pmtiles_url")
        payload["document"]["analysis:parsing:pmtiles_size"] = check.get("pmtiles_size")
    if config.CSV_TO_GEOJSON and check.get("geojson_url"):
        payload["document"]["analysis:parsing:geojson_url"] = check.get("geojson_url")
        payload["document"]["analysis:parsing:geojson_size"] = check.get("geojson_size")
    payload["document"] = UdataPayload(payload["document"])
    queue.enqueue(send, _priority="high", **payload)

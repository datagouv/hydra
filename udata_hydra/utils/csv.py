import json
import os

from csv_detective.explore_csv import routine as csv_detective_routine

from udata_hydra.utils.db import get_check, insert_csv_analysis
from udata_hydra.utils.file import download_resource


async def analyse_csv(check_id: int):
    """Launch csv analysis from a check"""
    check = await get_check(check_id)

    # ATM we (might) re-download the file, to avoid spaghetti code
    # TODO: find a way to mutualize with main analysis
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")
    tmp_file = await download_resource(url, headers)

    try:
        csv_inspection = await perform_csv_inspection(tmp_file.name)
        await insert_csv_analysis({
            "resource_id": check["resource_id"],
            "url": check["url"],
            "check_id": check_id,
            "csv_detective": csv_inspection
        })
    finally:
        os.remove(tmp_file.name)


async def csv_to_db(check_id: int):
    """Convert a csv to database table from check result"""
    pass


async def perform_csv_inspection(file_path):
    """Launch csv-detective against given file"""
    return csv_detective_routine(file_path)


async def detect_csv_from_headers(check) -> bool:
    """Determine if content-type header looks like a csv's one"""
    headers = json.loads(check["headers"] or {})
    return any([
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/csv", "text/plain", "text/csv"
        ]
    ])

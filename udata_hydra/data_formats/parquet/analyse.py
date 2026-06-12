import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats import Parquet
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    ParseException,
    Timer,
)

log = logging.getLogger("udata-hydra")

if TYPE_CHECKING:
    from udata_hydra.data_formats import Table


async def analyse_parquet(
    check: Record | dict,
    file: Parquet | None = None,
    debug_insert: bool = False,
) -> "Table|None":
    """Insert parquet file and metadata in db"""
    if not config.PARQUET_TO_DB:
        log.debug("PARQUET_TO_DB turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])

    await Resource.update(resource_id, {"status": "ANALYSING_PARQUET"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-parquet", resource_id)
    assert any(_ is not None for _ in (check["id"], check["url"]))

    if file is None:
        tmp_file = await helpers.read_or_download_file(
            check=check,
            filename=None,
            data_format=Parquet,
            exception=exception,
        )
        file = Parquet(
            path=tmp_file.name, resource_id=resource_id, dataset_id=check.get("dataset_id")
        )
    timer.mark("download-file")

    check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore[assignment]

    # open the file and read the metadata
    try:
        file.inspect()
    except Exception as e:
        raise ParseException(
            message=str(e),
            step="parquet_analysis",
            resource_id=resource_id,
            url=check["url"],
            check_id=check["id"],
        ) from e
    timer.mark("parquet-analysis")

    # Convert to PMTiles
    table = await file.to_db(check=check, table_indexes=table_indexes, debug_insert=debug_insert)
    timer.mark("parquet-to-db")
    check = await Check.update(  # type: ignore[assignment]
        check_id=check["id"],
        data={
            "parsing_finished_at": datetime.now(timezone.utc),
        },
    )
    return table

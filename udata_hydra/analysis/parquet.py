import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone

import pyarrow.parquet as pq
from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.tables_index import insert_tables_index_entry
from udata_hydra.conversion.parquet_to_db import parquet_to_db
from udata_hydra.conversion.schema import PYARROW_TYPE_TO_PYTHON
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
)

log = logging.getLogger("udata-hydra")


async def analyse_parquet(
    check: Record | dict,
    filename: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Insert parquet file and metadata in db"""
    if not config.PARQUET_TO_DB:
        log.debug("PARQUET_TO_DB turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]
    # Preserve dataset_id from original check record
    dataset_id = check.get("dataset_id")

    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_PARQUET"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-parquet", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    table_name, tmp_file = None, None
    try:
        tmp_file = await helpers.read_or_download_file(
            check=check,
            filename=filename,
            file_format="parquet",
            exception=exception,
        )
        table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore[assignment]

        # open the file and read the metadata
        try:
            parquet_file = pq.ParquetFile(tmp_file.name)
            columns = {}
            for col in parquet_file.schema_arrow:
                col_type = str(col.type)
                if col_type.startswith("dictionary"):
                    # dictionaries are for columns with repeated values
                    # we need to dig deeper to get the type
                    col_type = str(col.type.value_type)
                try:
                    columns[col.name] = next(
                        pytype
                        for pyartype, pytype in PYARROW_TYPE_TO_PYTHON.items()
                        if re.search(pyartype, col_type)
                    )
                except StopIteration:
                    raise ValueError(f"Unknown pyarrow type: {col.type}")
            inspection = {
                "columns": {
                    col_name: {
                        "format": pytype,
                        "python_type": pytype,
                    }
                    for col_name, pytype in columns.items()
                }
            }
            inspection["total_lines"] = parquet_file.metadata.num_rows
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="parquet_analysis",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e
        timer.mark("parquet-analysis")

        await parquet_to_db(
            parquet_file=parquet_file,
            inspection=inspection,
            table_name=table_name,
            table_indexes=table_indexes,
            resource_id=resource_id,
            debug_insert=debug_insert,
        )
        check = await Check.update(check["id"], {"parsing_table": table_name})  # type: ignore[assignment]
        timer.mark("parquet-to-db")

        check = await Check.update(  # type: ignore[assignment]
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
            },
        )
        await insert_tables_index_entry(table_name, inspection, check, dataset_id or "")

    except (ParseException, IOException) as e:
        check = await handle_parse_exception(e, table_name, check)  # type: ignore[assignment]
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})

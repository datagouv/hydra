import json
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.conversion.schema import PYARROW_TYPE_TO_PYTHON
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.table import Table
from udata_hydra.db.check import Check
from udata_hydra.db.codec import parse_json_value
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    ParseException,
    Timer,
    handle_parse_exception,
)

if TYPE_CHECKING:
    from udata_hydra.data_formats.table import Table

log = logging.getLogger("udata-hydra")


class Parquet(DataFormat):
    standard_mime_type = "application/vnd.apache.parquet"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["parquet"])
    check_url = "parquet"
    further_analysis = True

    def inspect(self) -> dict:
        file = pq.ParquetFile(self.path.as_posix())
        columns = {}
        self.inspection = {"header": []}
        for col in file.schema_arrow:
            self.inspection["header"].append(col.name)
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
        self.inspection["columns"] = {
            col_name: {
                "format": pytype,
                "python_type": pytype,
            }
            for col_name, pytype in columns.items()
        }
        self.inspection["total_lines"] = file.metadata.num_rows
        return self.inspection

    async def analyse(self, check: dict, debug_insert: bool = False) -> "Table|None":
        """Insert parquet file and metadata in db"""
        if not config.PARQUET_TO_DB:
            log.debug("PARQUET_TO_DB turned off, skipping.")
            return

        resource_id: str = str(check["resource_id"])

        resource = await Resource.set_job_status(resource_id, "parquet", "ANALYSING_PARQUET")

        # Check if the resource is in the exceptions table
        # If it is, get the table_indexes to use them later
        exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
        table_indexes: dict | None = None
        if exception and exception.get("table_indexes"):
            table_indexes = parse_json_value(exception["table_indexes"])

        timer = Timer("analyse-parquet", resource_id)
        assert any(_ is not None for _ in (check["id"], check["url"]))

        table = None
        try:
            check = await Check.update(
                check["id"], {"parsing_started_at": datetime.now(timezone.utc)}
            )  # type: ignore[assignment]

            # open the file and read the metadata
            try:
                self.inspect()
            except Exception as e:
                raise ParseException(
                    message=str(e),
                    step="parquet_analysis",
                    resource_id=resource_id,
                    url=check["url"],
                    check_id=check["id"],
                ) from e
            timer.mark("parquet-analysis")

            table = await self.to_db(
                check=check, table_indexes=table_indexes, debug_insert=debug_insert
            )
            timer.mark("parquet-to-db")
            check = await Check.update(  # type: ignore[assignment]
                check_id=check["id"],
                data={
                    "parsing_finished_at": datetime.now(timezone.utc),
                },
            )
            return table
        except ParseException as e:
            check = await handle_parse_exception(
                e, table.table_name if table is not None else None, check
            )  # type: ignore[assignment]
            return None
        finally:
            await helpers.notify_udata(resource, check)
            timer.stop()
            self.path.unlink()
            await Resource.clear_job_status(resource_id, "parquet")

    async def to_db(
        self, check: dict, table_indexes: dict[str, str] | None = None, debug_insert: bool = False
    ) -> "Table":
        from udata_hydra.data_formats.parquet.to_db import parquet_to_db

        return await parquet_to_db(
            file=self,
            check=check,
            table_indexes=table_indexes,
            debug_insert=debug_insert,
        )

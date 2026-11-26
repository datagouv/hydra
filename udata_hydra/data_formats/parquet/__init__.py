import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.table import Table
from udata_hydra.db.check import Check
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.db.resource_index import ResourceIndex
from udata_hydra.db.resource_job_status import ResourceJobStatus
from udata_hydra.utils import (
    ParseException,
    Timer,
    queue,
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

    async def inspect(self) -> dict:
        # csv-detective picks its engine from the content (the downloaded file has no extension)
        # and would read a CSV or a gzip served as a parquet resource: fail here instead, before
        # the table of the previous version is dropped
        pq.ParquetFile(self.path)
        # no validation against the previous inspection, unlike a CSV: csv-detective doesn't check
        # the column order of a parquet file and would return the previous one, while the rows
        # are copied to the table in the order of the file
        self.inspection = helpers.run_csv_detective(self, previous_inspection=None)
        return self.inspection

    async def analyse(
        self, check: dict, debug_insert: bool = False, force_analysis: bool = False
    ) -> "Table|None":
        """Insert parquet file and metadata in db"""
        if not config.PARQUET_TO_DB:
            log.debug("PARQUET_TO_DB turned off, skipping.")
            return

        resource_id: str = str(check["resource_id"])

        await ResourceJobStatus.set(resource_id, "parquet", "ANALYSING_PARQUET")

        table_indexes: dict[str, str] = await ResourceIndex.get_by_resource_id(resource_id)

        timer = Timer("analyse-parquet", resource_id)
        assert any(_ is not None for _ in (check["id"], check["url"]))

        try:
            check = await Check.update(
                check["id"], {"parsing_started_at": datetime.now(timezone.utc)}
            )  # type: ignore[assignment]

            # analyse the whole file with csv-detective
            try:
                await self.inspect()
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
        finally:
            timer.stop()
            await ResourceJobStatus.clear(resource_id, "parquet")
            self.path.unlink(missing_ok=True)

        # the file of the producer is exported again from the table, like a CSV: the readers of
        # the exported files (api-tabular) get the embedded inspection and the rows in __id order
        if config.DB_TO_PARQUET:
            from udata_hydra.analysis.exports import export_parquet

            # after the clear above, which shares the "parquet" job key
            await ResourceJobStatus.set(resource_id, "parquet", "CONVERTING_TO_PARQUET")
            exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
            queue.enqueue(
                export_parquet,
                table=table,
                check=dict(check),
                _priority="low",
                _exception=bool(exception),
            )
        return table

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

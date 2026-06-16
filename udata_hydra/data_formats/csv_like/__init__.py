import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from asyncpg import Record
from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.tables_index import get_previous_inspection
from udata_hydra.data_formats.csv_like.to_geojson import _detect_geo_columns
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
    queue,
)

if TYPE_CHECKING:
    from udata_hydra.data_formats import Geojson, Table

log = logging.getLogger("udata-hydra")


class CsvLike(DataFormat):
    further_analysis = True

    async def inspect(self) -> dict:
        previous_inspection: dict | None = (
            await get_previous_inspection(resource_id=self.resource_id)
            if self.resource_id
            else None
        )
        if previous_inspection:
            if self.resource_id:
                await Resource.update(self.resource_id, {"status": "VALIDATING_CSV"})
            self.inspection = validate_then_detect(  # ty: ignore[invalid-assignment]
                file_path=self.path.as_posix(),
                previous_analysis=previous_inspection,
                output_profile=True,
                num_rows=-1,
                save_results=False,
            )
        else:
            self.inspection = csv_detective_routine(  # ty: ignore[invalid-assignment]
                file_path=self.path.as_posix(),
                output_profile=True,
                num_rows=-1,
                save_results=False,
            )
        return self.inspection

    async def analyse(self, check: dict, debug_insert: bool = False) -> None:
        """Launch csv analysis from a check or an URL (debug), using previously downloaded file if any"""
        if not config.CSV_ANALYSIS:
            log.debug("CSV_ANALYSIS turned off, skipping.")
            return

        resource_id: str = str(check["resource_id"])

        # Update resource status to ANALYSING_CSVLIKE
        resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

        # Check if the resource is in the exceptions table
        # If it is, get the table_indexes to use them later
        exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

        table_indexes: dict | None = None
        if exception and exception.get("table_indexes"):
            table_indexes = json.loads(exception["table_indexes"])

        timer = Timer("analyse-csv", resource_id)
        assert any(_ is not None for _ in (check["id"], check["url"]))

        table = None
        try:
            check = await Check.update(
                check["id"], {"parsing_started_at": datetime.now(timezone.utc)}
            )  # type: ignore[assignment]
            # Launch csv-detective against given file
            try:
                await self.inspect()
            except Exception as e:
                raise ParseException(
                    message=str(e),
                    step="csv_detective",
                    resource_id=resource_id,
                    url=check["url"],
                    check_id=check["id"],
                ) from e
            timer.mark("csv-inspection")

            if not config.CSV_TO_DB:
                log.debug(
                    "CSV_TO_DB is off, skipping Postgres parsing table ingest and deferred export jobs."
                )
            else:
                table: Table = await self.to_db(
                    check=check,
                    table_indexes=table_indexes,
                    debug_insert=debug_insert,
                )
                timer.mark("csv-to-db")

            check = await Check.update(  # type: ignore[assignment]
                check_id=check["id"],
                data={
                    "parsing_finished_at": datetime.now(timezone.utc),
                },
            )

            if config.CSV_TO_DB:
                if (
                    config.DB_TO_PARQUET
                    and int(table.inspection.get("total_lines", 0)) >= config.MIN_LINES_FOR_PARQUET  # type: ignore[arg-type]
                ):
                    from udata_hydra.analysis.exports import export_parquet

                    queue.enqueue(
                        export_parquet,
                        table=table,
                        check=dict(check),
                        _priority="low",
                        _exception=bool(exception),
                    )

                if (
                    config.DB_TO_GEOJSON
                    and table is not None
                    and _detect_geo_columns(table.inspection) is not None
                ):
                    from udata_hydra.analysis.exports import export_geojson_pmtiles

                    queue.enqueue(
                        export_geojson_pmtiles,
                        source=table,
                        check=dict(check),
                        _priority="low",
                        _exception=bool(exception),
                    )

        except (ParseException, IOException) as e:
            check = await handle_parse_exception(
                e, table.table_name if table is not None else None, check
            )  # type: ignore[assignment]
        finally:
            await helpers.notify_udata(resource, check)
            timer.stop()
            self.path.unlink()

            # Reset resource status to None
            await Resource.update(resource_id, {"status": None})

    async def to_db(
        self, check: dict, table_indexes: dict[str, str] | None = None, debug_insert: bool = False
    ) -> "Table":
        from udata_hydra.data_formats.csv_like.to_db import csv_to_db

        if not config.CSV_TO_DB:
            log.debug(
                "CSV_TO_DB is off, skipping Postgres parsing table ingest and deferred export jobs."
            )
        return await csv_to_db(
            file=self,
            check=check,
            table_indexes=table_indexes,
            debug_insert=debug_insert,
        )

    async def to_geojson(self) -> "Geojson|None ":
        from udata_hydra.data_formats.csv_like.to_geojson import csv_to_geojson

        return await csv_to_geojson(file=self)


class Csv(CsvLike):
    standard_mime_type = "text/csv"
    valid_mime_types = {
        standard_mime_type,
        "application/csv",
        "text/plain",
    }
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["csv"])


class Csvgz(CsvLike):
    standard_mime_type = "application/gzip"
    valid_mime_types = {
        standard_mime_type,
        "application/octet-stream",
        "application/x-gzip",
    }
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["csvgz"])
    check_url = "csv.gz"

    @classmethod
    def detect_from_catalog_format(cls, format: str | None) -> bool:
        return format == "csv.gz"


class Xls(CsvLike):
    standard_mime_type = "application/vnd.ms-excel"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["xls"])


class Xlsx(CsvLike):
    standard_mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["xlsx"])

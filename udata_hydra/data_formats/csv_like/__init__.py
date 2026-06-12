import logging
from typing import TYPE_CHECKING

from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect

from udata_hydra import config
from udata_hydra.analysis.tables_index import get_previous_inspection
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.resource import Resource

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

    async def analyse(self, check: dict, debug_insert: bool = False):
        from udata_hydra.data_formats.csv_like.analyse import analyse_csv

        return await analyse_csv(check=check, file=self, debug_insert=debug_insert)

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

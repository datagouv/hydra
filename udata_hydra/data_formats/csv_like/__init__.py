from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect

from udata_hydra import config
from udata_hydra.analysis.tables_index import get_previous_analysis
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.resource import Resource


class CsvLike(DataFormat):
    further_analysis = True

    async def analyse(self) -> None:
        if not self.resource_id:
            return
        previous_analysis: dict | None = await get_previous_analysis(resource_id=self.resource_id)
        if previous_analysis:
            await Resource.update(self.resource_id, {"status": "VALIDATING_CSV"})
            self.inspection = validate_then_detect(
                file_path=self.path.name,
                previous_analysis=previous_analysis,
                output_profile=True,
                num_rows=-1,
                save_results=False,
            )
        else:
            self.inspection = csv_detective_routine(
                file_path=self.path.name,
                output_profile=True,
                num_rows=-1,
                save_results=False,
            )


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
    def detect_from_catalog_format(cls, format: str) -> bool:
        return format == "csv.gz"


class Xls(CsvLike):
    standard_mime_type = "application/vnd.ms-excel"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["xls"])


class Xlsx(CsvLike):
    standard_mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["xlsx"])

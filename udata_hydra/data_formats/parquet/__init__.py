import re
import pyarrow.parquet as pq

from udata_hydra import config, context
from udata_hydra.conversion.schema import PYARROW_TYPE_TO_PYTHON
from udata_hydra.data_formats.data_format import DataFormat
# from udata_hydra.data_formats.parquet.to_db import parquet_to_db


class Parquet(DataFormat):

    standard_mime_type = "application/vnd.apache.parquet"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["parquet"])
    file: pq.ParquetFile
    check_url = "parquet"

    @classmethod
    async def detect_from_catalog(cls, check: dict) -> bool:
        pool = await context.pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT format FROM catalog WHERE resource_id = $1", f"{check['resource_id']}"
            )
        return row["format"] == "parquet"

    def analyse(self) -> None:
        self.file = pq.ParquetFile(self.path.as_posix())
        columns = {}
        for col in self.file.schema_arrow:
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
        self.inspection = {
            "columns": {
                col_name: {
                    "format": pytype,
                    "python_type": pytype,
                }
                for col_name, pytype in columns.items()
            }
        }
        self.inspection["total_lines"] = self.file.metadata.num_rows

    async def to(self, target_format: str, **kwargs) -> DataFormat | None:
        match target_format:
            case "db":
                # return await parquet_to_db(
                #     parquet_file=self.file,
                #     inspection=self.inspection,
                #     **kwargs,
                # )
                return
            case _:
                raise NotImplementedError

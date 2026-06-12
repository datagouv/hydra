import re
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from udata_hydra import config
from udata_hydra.conversion.schema import PYARROW_TYPE_TO_PYTHON
from udata_hydra.data_formats.data_format import DataFormat

if TYPE_CHECKING:
    from udata_hydra.data_formats.table import Table


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

    async def analyse(self, check: dict, debug_insert: bool = False):
        from udata_hydra.data_formats.parquet.analyse import analyse_parquet

        return await analyse_parquet(check=check, file=self, debug_insert=debug_insert)

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

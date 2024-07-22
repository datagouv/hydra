import csv as stdcsv
from io import BytesIO
from typing import Generator

import openpyxl
import xlrd


def generate_dialect(inspection: dict) -> stdcsv.Dialect:
    class CustomDialect(stdcsv.unix_dialect):
        # TODO: it would be nice to have more info from csvdetective to feed the dialect
        # in the meantime we might want to sniff the file a bit
        delimiter = inspection["separator"]

    return CustomDialect()


class Reader:
    def __init__(self, file_path, inspection):
        self.file_path = file_path
        self.inspection = inspection
        self.nb_skip = self.inspection["header_row_idx"]
        self.mapping = {
            "openpyxl": "iter_rows",
            "xlrd": "get_rows",
        }
        self.nb_columns = len(self.inspection["header"])
        self.reader = None

    def __enter__(self):
        if self.inspection.get("engine") == "openpyxl":
            with open(self.file_path, "rb") as f:
                content = BytesIO(f.read())
            self.file = openpyxl.load_workbook(content)
            self.sheet = self.file[self.inspection["sheet_name"]]
            self.reader = self._excel_reader()

        elif self.inspection.get("engine") == "xlrd":
            self.file = xlrd.open_workbook(self.file_path)
            self.sheet = self.file[self.inspection["sheet_name"]]
            self.reader = self._excel_reader()

        else:
            self.file = open(self.file_path, encoding=self.inspection["encoding"])
            self.reader = stdcsv.reader(
                self._skip_rows(), dialect=generate_dialect(self.inspection)
            )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file is not None and hasattr(self.file, "close"):
            self.file.close()

    def _skip_rows(self):
        # skipping header
        for _ in range(self.nb_skip + 1):
            next(self.file)
        return self.file

    def _excel_reader(self) -> Generator:
        _method = getattr(self.sheet, self.mapping[self.inspection["engine"]])
        for idx, row in enumerate(_method()):
            # skipping header
            if idx <= self.nb_skip:
                continue
            yield [c.value for c in row]

    def __iter__(self):
        return self.reader

import csv as stdcsv
import openpyxl
import xlrd
from odf.opendocument import load
from odf import text, table, teletype
from io import BytesIO


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
        self.reader = None

    def __enter__(self):
        if self.inspection.get('engine') == 'openpyxl':
            with open(self.file_path, 'rb') as f:
                content = BytesIO(f.read())
            self.file = openpyxl.load_workbook(content)
            self.sheet = self.file[self.inspection['sheet_name']]
            self.reader = self._excel_reader()

        elif self.inspection.get('engine') == 'xlrd':
            self.file = xlrd.open_workbook(self.file_path)
            self.sheet = self.file[self.inspection['sheet_name']]
            self.reader = self._excel_reader()

        elif self.inspection.get('engine') == 'odf':
            self.file = load(self.file_path)
            self.sheet = [
                s for s in self.file.spreadsheet.getElementsByType(table.Table)
                if s.getAttribute('name') == self.inspection['sheet_name']
            ][0]
            self.reader = self._ods_reader()

        else:
            self.file = open(self.file_path, encoding=self.inspection["encoding"])
            self.reader = stdcsv.reader(
                self._skip_rows(),
                dialect=generate_dialect(self.inspection)
            )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file is not None and hasattr(self.file, 'close'):
            self.file.close()

    def _skip_rows(self):
        # skipping header
        nb_skip = self.inspection.get("header_row_idx", 0) + 1
        for _ in range(nb_skip):
            next(self.file)
        return self.file

    def _excel_reader(self):
        mapping = {
            'openpyxl': 'iter_rows',
            'xlrd': 'get_rows',
        }
        _method = getattr(self.sheet, mapping[self.inspection['engine']])
        for idx, row in enumerate(_method()):
            # skipping header
            if idx == 0:
                continue
            yield [c.value for c in row]

    def _ods_reader(self):
        nb_columns = len(self.inspection['header'])
        for idx, row in enumerate(self.sheet.getElementsByType(table.TableRow)):
            # skipping header
            if idx == 0:
                continue
            row_data = []
            for cell in row.getElementsByType(table.TableCell):
                cell_text = ""
                for paragraph in cell.getElementsByType(text.P):
                    cell_text += teletype.extractText(paragraph)
                row_data.append(cell_text.strip())
            # handling end of file
            if all([not c for c in row_data]):
                break
            if len(row_data) > nb_columns:
                if row_data[-1]:
                    raise ValueError('A row has more fields than the number of columns of the file.')
                else:
                    # handling case where reader considers one column too many but it's empty
                    row_data = row_data[:-1]
            # handling empty last column(s)
            if len(row_data) < nb_columns:
                row_data += [''] * (nb_columns - len(row_data))
            yield row_data

    def __iter__(self):
        return self.reader

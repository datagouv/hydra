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


def process_ods(value):
    # OK this is messed up:
    # csv-detective reads ods files with pandas, and columns with percentages
    # appear directly as numbers between 0 and 1(even when loaded as str)
    # so csv-detective processes them as floats (which I think is what they are, how they are stored)
    # BUT loading the *same* file with odfpy returns percentages as strings for the *same* cells
    # for instance where pandas reads "0.215", odfpy reads "21.5%" or even "22%"
    # I've tried a lot of things but I can't manage to load the float values with odfpy
    # I dont think this shoud be solved on csv-detective's side, the values *are* floats
    # so a couple of options (non-exhaustive):
    #   - process the percentages to cast them as floats in smart_cast
    #   - process the percentages to store them as floats when loaded (which is what this function does)
    #   - use pandas to load ods files in here (do we want pandas in hydra)
    #   - store them as strings in db? (what about the column type)
    #   - default to null (but the column has been check and is safely castable, that's a shame)
    # but who knows maybe ods does the same kind of trick for other types...
    if not value:
        return value
    no_blank = value.replace(' ', '')
    if no_blank and all([k.isnumeric() for k in no_blank[:-1]]) and no_blank[-1] == "%":
        return float(value[:-1]) / 100
    return value


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
                row_data.append(process_ods(cell_text.strip()))
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

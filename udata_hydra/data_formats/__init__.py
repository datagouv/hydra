from udata_hydra.data_formats.csv_like import Csv, Csvgz, Xls, Xlsx
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.geojson import Geojson
from udata_hydra.data_formats.parquet import Parquet
from udata_hydra.data_formats.pmtiles import PMTiles  # noqa
from udata_hydra.data_formats.table import Table


async def detect_data_format_from_check(check: dict) -> DataFormat | None:
    for fmt in [
        Csv,
        Csvgz,
        Xls,
        Xlsx,
        Geojson,
        Parquet,
        Table,
    ]:
        if fmt.detect_from_check(check) or await fmt.detect_from_catalog(check):
            return fmt
    return None

from asyncpg import Record

from udata_hydra import context
from udata_hydra.data_formats.csv_like import Csv, Csvgz, Xls, Xlsx
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.geojson import Geojson
from udata_hydra.data_formats.ogc import Wfs, Wms
from udata_hydra.data_formats.parquet import Parquet


async def detect_data_format_from_check_or_catalog(check: dict) -> type[DataFormat] | None:
    pool = await context.pool()
    async with pool.acquire() as connection:
        row: Record = await connection.fetchrow(
            "SELECT format FROM catalog WHERE resource_id = $1", f"{check['resource_id']}"
        )
    resource_format = row["format"] if row is not None else None
    for fmt in [
        Csv,
        Csvgz,
        Xls,
        Xlsx,
        Geojson,
        Parquet,
        Wfs,
        Wms,
    ]:
        if fmt.detect_from_check(
            check, resource_format=resource_format
        ) or fmt.detect_from_catalog_format(resource_format):
            return fmt
    return None

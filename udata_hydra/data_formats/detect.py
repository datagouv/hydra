import json

from asyncpg import Record

from udata_hydra import context
from udata_hydra.data_formats.csv_like import Csv, Xls, Xlsx
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.data_formats.geojson import Geojson
from udata_hydra.data_formats.gz import Gz
from udata_hydra.data_formats.ogc import Wfs, Wms
from udata_hydra.data_formats.parquet import Parquet

PAYLOAD_FORMATS: tuple[type[DataFormat], ...] = (Csv, Xls, Xlsx, Geojson, Parquet)


def _strip_gz_suffix(value: str | None) -> str | None:
    if value and value.lower().endswith(".gz"):
        return value[:-3]
    return value


def detect_format_from_payload(
    check: dict, payload_mime: str | None, resource_format: str | None = None
) -> type[DataFormat] | None:
    """Classify an already uncompressed gzip payload (MIME + inner URL / catalog format)."""
    mime = (payload_mime or "").split(";")[0].strip()
    inner_check = {
        **check,
        "url": _strip_gz_suffix(check.get("url")) or "",
        "headers": json.dumps({"content-type": mime}),
    }
    inner_format = _strip_gz_suffix(resource_format)
    for fmt in PAYLOAD_FORMATS:
        if fmt.detect_from_check(
            inner_check, resource_format=inner_format
        ) or fmt.detect_from_catalog_format(inner_format):
            return fmt
    return None


async def catalog_format_for(resource_id: str | None) -> str | None:
    if not resource_id:
        return None
    pool = await context.pool()
    async with pool.acquire() as connection:
        row: Record | None = await connection.fetchrow(
            "SELECT format FROM catalog WHERE resource_id = $1", f"{resource_id}"
        )
    return row["format"] if row is not None else None


async def detect_data_format_from_check_or_catalog(check: dict) -> type[DataFormat] | None:
    resource_format = await catalog_format_for(check.get("resource_id"))
    for fmt in [
        Csv,
        Gz,
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

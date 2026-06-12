import logging
from typing import TYPE_CHECKING

from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.utils.s3 import S3Client

if TYPE_CHECKING:
    from udata_hydra.data_formats import Geojson, Parquet

log = logging.getLogger("udata-hydra")

_parquet_s3_client = S3Client(bucket=config.S3_BUCKET)


class Table(DataFormat):
    @classmethod
    def detect_from_check(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def detect_from_catalog_format(cls, **kwargs):
        raise NotImplementedError

    def analyse(self, **kwargs):
        raise NotImplementedError

    async def to_parquet(self) -> "Parquet|None":
        from udata_hydra.data_formats.table.to_parquet import db_to_parquet

        if (
            config.DB_TO_PARQUET
            and int(self.inspection.get("total_lines", 0)) >= config.MIN_LINES_FOR_PARQUET  # type: ignore[arg-type]
        ):
            return await db_to_parquet(table=self)

    async def to_geojson(self) -> "Geojson|None":
        from udata_hydra.data_formats.table.to_geojson import db_to_geojson

        if config.DB_TO_GEOJSON:
            return await db_to_geojson(table=self)

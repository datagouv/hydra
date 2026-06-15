import logging
from typing import TYPE_CHECKING

from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat

if TYPE_CHECKING:
    from udata_hydra.data_formats import Geojson, Parquet

log = logging.getLogger("udata-hydra")


class Table(DataFormat):
    @classmethod
    def detect_from_check(cls, check: dict, **kwargs):
        raise NotImplementedError

    @classmethod
    def detect_from_catalog_format(cls, format: str | None):
        raise NotImplementedError

    async def analyse(self, check: dict):
        raise NotImplementedError

    async def to_parquet(self) -> "Parquet|None":
        from udata_hydra.data_formats.table.to_parquet import db_to_parquet

        if (
            config.DB_TO_PARQUET
            and int(self.inspection.get("total_lines", 0)) >= config.MIN_LINES_FOR_PARQUET
        ):
            return await db_to_parquet(table=self)

    async def to_geojson(self) -> "Geojson|None":
        from udata_hydra.data_formats.table.to_geojson import db_to_geojson

        if config.DB_TO_GEOJSON:
            return await db_to_geojson(table=self)

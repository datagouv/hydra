from typing_extensions import override

from udata_hydra.data_formats.data_format import DataFormat
# from udata_hydra.data_formats.table.to_geojson import db_to_geojson
# from udata_hydra.data_formats.table.to_parquet import db_to_parquet


class Table(DataFormat):

    @override
    def detect_from_check(cls, **kwargs):
        raise NotImplementedError

    def analyse(self, **kwargs):
        raise NotImplementedError

    async def to(self, target_format: str, **kwargs) -> DataFormat | None:
        match target_format:
            case "geojson":
                # return await db_to_geojson(**kwargs)
                return
            case "parquet":
                # return await db_to_parquet(**kwargs)
                return
            case _:
                raise NotImplementedError
from udata_hydra import config, context
from udata_hydra.data_formats.data_format import DataFormat
# from udata_hydra.data_formats.geojson.to_pmtiles import geojson_to_pmtiles


class Geojson(DataFormat):

    standard_mime_type = "application/vnd.geo+json"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["geojson"])
    check_url = "geojson"

    @classmethod
    async def detect_from_catalog(cls, check: dict) -> bool:
        pool = await context.pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT format FROM catalog WHERE resource_id = $1", f"{check['resource_id']}"
            )
        return row["format"] == "geojson"

    def analyse(self, **kwargs):
        raise NotImplementedError

    async def to(self, target_format: str, **kwargs) -> DataFormat | None:
        match target_format:
            case "pmtiles":
                # return await geojson_to_pmtiles(**kwargs)
                return
            case _:
                raise NotImplementedError

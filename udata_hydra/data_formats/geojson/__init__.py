from typing import TYPE_CHECKING

from udata_hydra import config
from udata_hydra.data_formats.data_format import DataFormat

if TYPE_CHECKING:
    from udata_hydra.data_formats.pmtiles import PMTiles


class Geojson(DataFormat):
    standard_mime_type = "application/vnd.geo+json"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["geojson"])
    check_url = "geojson"
    further_analysis = True

    async def analyse(self, check: dict):
        from udata_hydra.data_formats.geojson.analyse import analyse_geojson

        await analyse_geojson(file=self, check=check)

    async def to_pmtiles(self) -> "PMTiles":
        from udata_hydra.data_formats.geojson.to_pmtiles import geojson_to_pmtiles

        return geojson_to_pmtiles(self)

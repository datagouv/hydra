import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats.data_format import DataFormat
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException, Timer, handle_parse_exception

if TYPE_CHECKING:
    from udata_hydra.data_formats.pmtiles import PMTiles

log = logging.getLogger("udata-hydra")


class Geojson(DataFormat):
    standard_mime_type = "application/vnd.geo+json"
    valid_mime_types = {standard_mime_type}
    max_filesize_allowed = int(config.MAX_FILESIZE_ALLOWED["geojson"])
    check_url = "geojson"
    further_analysis = True

    async def analyse(self, check: dict) -> None:
        """Launch GeoJSON analysis from a check or an URL (debug), using previously downloaded file if any"""
        if not config.GEOJSON_TO_PMTILES:
            log.debug("GEOJSON_TO_PMTILES turned off, skipping.")
            return

        from udata_hydra.analysis.exports import export_pmtiles

        resource_id: str = str(check["resource_id"])
        url = check["url"]

        resource = await Resource.set_job_status(resource_id, "geojson", "ANALYSING_GEOJSON")

        timer = Timer("analyse-geojson", resource_id)
        assert any(_ is not None for _ in (check["id"], url))

        try:
            await export_pmtiles(geojson_file=self, check=check)
            timer.mark("geojson-to-pmtiles")
            check = await Check.update(  # type: ignore[assignment]
                check_id=check["id"],
                data={
                    "parsing_finished_at": datetime.now(timezone.utc),
                },
            )
        except ParseException as e:
            check = await handle_parse_exception(e, None, check)  # type: ignore[assignment]
        finally:
            await helpers.notify_udata(resource, check)
            timer.stop()
            self.path.unlink()
            await Resource.clear_job_status(resource_id, "geojson")

    async def to_pmtiles(self) -> "PMTiles":
        from udata_hydra.data_formats.geojson.to_pmtiles import geojson_to_pmtiles

        return geojson_to_pmtiles(self)

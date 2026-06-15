import logging
from typing import TYPE_CHECKING

import tippecanoe

from udata_hydra.data_formats import Geojson
from udata_hydra.utils import true_path

if TYPE_CHECKING:
    from udata_hydra.data_formats import PMTiles

DEFAULT_PMTILES_FILENAME = "converted_from_db.pmtiles"
log = logging.getLogger("udata-hydra")


def geojson_to_pmtiles(file: Geojson) -> "PMTiles":
    """
    Convert a GeoJSON file to PMTiles file and optionally upload to S3-compatible storage.

    Args:
        file: a Geojson instance.

    Returns:
        pmtiles_file: a PMTiles instance.
    """
    from udata_hydra.data_formats import PMTiles

    pmtiles_name = (
        f"{file.resource_id}.pmtiles" if file.resource_id is not None else DEFAULT_PMTILES_FILENAME
    )
    log.debug(f"Converting GeoJSON file '{file.file_name}' to PMTiles file '{pmtiles_name}'")

    command = [
        "--maximum-zoom=g",  # guess
        "-o",
        true_path(pmtiles_name),
        "--force",  # don't crash if output file already exists, override it
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        true_path(file.file_name),
    ]
    exit_code = tippecanoe._program("tippecanoe", *command)
    if exit_code:
        raise ValueError(f"GeoJSON to PMTiles conversion failed with exit code {exit_code}")
    log.debug(f"Successfully converted {file.file_name} to {pmtiles_name}")

    return PMTiles(file_name=pmtiles_name, resource_id=file.resource_id, dataset_id=file.dataset_id)

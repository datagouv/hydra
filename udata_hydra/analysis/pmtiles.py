import logging
import os
from pathlib import Path

import tippecanoe

from udata_hydra import config
from udata_hydra.utils.minio import MinIOClient

DEFAULT_PMTILES_FILEPATH = Path("converted_from_geojson.pmtiles")

log = logging.getLogger("udata-hydra")

minio_client_pmtiles = MinIOClient(
    bucket=config.MINIO_PMTILES_BUCKET, folder=config.MINIO_PMTILES_FOLDER
)


async def geojson_to_pmtiles(
    input_file_path: Path,
    output_file_path: Path,
    upload_to_minio: bool = True,
) -> tuple[int, str | None]:
    """
    Convert a GeoJSON file to PMTiles file and optionally upload to MinIO.

    Args:
        input_file_path: GeoJSON file path to convert.
        output_file_path: Path where the PMTiles file should be saved.
        upload_to_minio: Whether to upload to MinIO (default: True).

    Returns:
        pmtiles_size: size of the PMTiles file.
        pmtiles_url: URL of the PMTiles file on MinIO. None if it was not uploaded to MinIO.
    """

    log.debug(f"Converting GeoJSON file '{input_file_path}' to PMTiles file '{output_file_path}'")

    command = [
        "--maximum-zoom=g",  # guess
        "-o",
        str(output_file_path),
        "--force",  # don't crash if output file already exists, override it
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        str(input_file_path),
    ]
    exit_code = tippecanoe._program("tippecanoe", *command)
    if exit_code:
        raise ValueError(f"GeoJSON to PMTiles conversion failed with exit code {exit_code}")
    log.debug(f"Successfully converted {input_file_path} to {output_file_path}")

    pmtiles_size: int = os.path.getsize(output_file_path)

    if upload_to_minio:
        log.debug(f"Sending PMTiles file {output_file_path} to MinIO")
        pmtiles_url = minio_client_pmtiles.send_file(
            str(output_file_path), delete_source=config.REMOVE_GENERATED_FILES
        )
    else:
        pmtiles_url = None

    return pmtiles_size, pmtiles_url

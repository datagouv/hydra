import logging
import os
from pathlib import Path

import tippecanoe

from udata_hydra import config
from udata_hydra.utils.s3 import S3Client

log = logging.getLogger("udata-hydra")

s3_client_pmtiles = S3Client(bucket=config.S3_BUCKET)


async def geojson_to_pmtiles(
    input_file_path: Path,
    output_file_path: Path,
    upload_to_s3: bool = True,
) -> tuple[int, str | None]:
    """
    Convert a GeoJSON file to PMTiles file and optionally upload to S3-compatible storage.

    Args:
        input_file_path: GeoJSON file path to convert.
        output_file_path: Path where the PMTiles file should be saved.
        upload_to_s3: Whether to upload to S3 (default: True).

    Returns:
        pmtiles_size: size of the PMTiles file.
        pmtiles_url: Public URL of the PMTiles object. None if it was not uploaded.
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

    if upload_to_s3:
        log.debug(f"Uploading PMTiles file {output_file_path} to S3")
        pmtiles_url = s3_client_pmtiles.send_file(
            str(output_file_path), delete_source=config.REMOVE_GENERATED_FILES
        )
    else:
        pmtiles_url = None

    return pmtiles_size, pmtiles_url

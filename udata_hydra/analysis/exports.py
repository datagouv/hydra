"""
Low-priority RQ jobs: exports from parsed DB tables (Parquet, GeoJSON + PMTiles).

Wrappers delegate to existing conversion helpers and own notify_udata,
ParseException handling, and resource status cleanup.
"""

import logging

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.data_formats import DataFormat, Geojson, Table
from udata_hydra.data_formats.geojson import Geojson
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException, handle_parse_exception, remove_remainders
from udata_hydra.utils.s3 import S3Client

log = logging.getLogger("udata-hydra")

s3_client = S3Client(bucket=config.S3_BUCKET)


async def _run_export_job(
    data_object: DataFormat,
    check: dict,
    step: str,
    remainder_types: list[str],
    export_fn: str,
    upload_to_s3: bool = True,
    delete_output: bool = False,
    delete_input: bool = True,
) -> DataFormat:
    check_out = None
    try:
        output: DataFormat = await getattr(data_object, export_fn)()
        if upload_to_s3:
            df_name = data_object.__class__.__name__
            log.debug(f"Uploading {df_name} file {data_object.path} to S3")
            upload_url = s3_client.send_file(
                data_object, delete_source=delete_output and config.REMOVE_GENERATED_FILES
            )
            await Check.update(
                check["id"],
                {
                    f"{df_name.lower()}_url": upload_url,
                    f"{df_name.lower()}_size": output.filesize,
                },
            )
            if delete_input and config.REMOVE_GENERATED_FILES:
                data_object.path.unlink()
    except Exception as e:
        if data_object.resource_id:
            remove_remainders(data_object.resource_id, remainder_types)
        check = await Check.get_by_id(check["id"])
        try:
            raise ParseException(
                message=str(e),
                step=step,
                resource_id=data_object.resource_id,
                url=check["url"],
                check_id=check["id"],
            ) from e
        except ParseException as pe:
            check_out = await handle_parse_exception(
                pe, getattr(data_object, "table_name", None), check
            )
    else:
        check_out = await Check.get_by_id(check["id"])
    finally:
        if data_object.resource_id is not None:
            resource = await Resource.get(data_object.resource_id)
            if resource is not None and check_out is not None:
                await helpers.notify_udata(resource, check_out)
            await Resource.update(data_object.resource_id, {"status": None})
        return output


async def export_parquet(
    table: Table,
    check: dict,
) -> None:
    """RQ target: parquet export for a db table."""
    await _run_export_job(
        data_object=table,
        check=check,
        step="parquet_export",
        remainder_types=["parquet"],
        export_fn="to_parquet",
    )


async def export_pmtiles(
    geojson_file: Geojson,
    check: dict,
) -> None:
    """RQ target: PMTiles export for a geojson."""
    await _run_export_job(
        data_object=geojson_file,
        check=check,
        step="pmtiles_export",
        remainder_types=["geojson", "pmtiles", "pmtiles-journal"],
        export_fn="to_pmtiles",
    )


async def export_geojson_pmtiles(
    table: Table,
    check: dict,
) -> None:
    """RQ target: GeoJSON + PMTiles export for a db table."""
    geojson_file = await _run_export_job(
        data_object=table,
        check=check,
        delete_output=False,
        step="geojson_export",
        remainder_types=["geojson"],
        export_fn="to_geojson",
    )
    await _run_export_job(
        data_object=geojson_file,
        check=check,
        delete_input=True,
        step="pmtiles_export",
        remainder_types=["geojson", "pmtiles", "pmtiles-journal"],
        export_fn="to_pmtiles",
    )

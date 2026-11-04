"""
Low-priority RQ jobs: exports from parsed DB tables (Parquet, GeoJSON + PMTiles).

Wrappers delegate to existing conversion helpers and own notify_udata,
ParseException handling, and resource status cleanup.
"""

import logging

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.data_formats import CsvLike, DataFormat, Geojson, Table
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_job_status import ResourceJobStatus
from udata_hydra.utils import ParseException, handle_parse_exception, remove_remainders

log = logging.getLogger("udata-hydra")


async def _run_export_job(
    data_object: DataFormat,
    check: dict,
    step: str,
    remainder_types: list[str],
    export_fn: str,
    job_keys: tuple[str, ...],
    delete_output: bool = False,
    delete_input: bool = True,
) -> DataFormat | None:
    output, check_out = None, None
    try:
        output: DataFormat = await getattr(data_object, export_fn)()
        df_name = output.__class__.__name__
        log.debug(f"Uploading {df_name} file {output.file_name} to S3")
        upload_url = context.s3_client().send_file(
            output, delete_source=delete_output and config.REMOVE_GENERATED_FILES
        )
        # the updated check may not be the last one anymore, e.g. recrawled since its parsing
        check_out = await Check.update(
            check["id"],
            {
                f"{df_name.lower()}_url": upload_url,
                f"{df_name.lower()}_size": output.filesize,
            },
        )
        if (
            delete_input
            and config.REMOVE_GENERATED_FILES
            and getattr(data_object, "file_name", None)
        ):
            data_object.path.unlink()
    except Exception as e:
        if data_object.resource_id:
            remove_remainders(data_object.resource_id, remainder_types)
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
    finally:
        # a later check has (re)parsed the resource: its analysis and status take precedence
        if data_object.resource_id is not None and not await Check.has_later_parsing(
            data_object.resource_id, check["id"]
        ):
            resource = await Resource.get(data_object.resource_id)
            if resource is not None and check_out is not None:
                await helpers.notify_udata(resource, check_out)
        if data_object.resource_id is not None:
            for job in job_keys:
                await ResourceJobStatus.clear(data_object.resource_id, job)
    return output


async def export_parquet(
    table: Table,
    check: dict,
) -> None:
    """RQ target: parquet export for a db table."""
    if table.resource_id:
        await ResourceJobStatus.set(table.resource_id, "parquet", "CONVERTING_TO_PARQUET")
    await _run_export_job(
        data_object=table,
        check=check,
        step="parquet_export",
        remainder_types=["parquet"],
        export_fn="to_parquet",
        job_keys=("parquet",),
    )


async def export_pmtiles(
    geojson_file: Geojson,
    check: dict,
) -> None:
    """RQ target: PMTiles export for a geojson."""
    if geojson_file.resource_id:
        await ResourceJobStatus.set(geojson_file.resource_id, "pmtiles", "CONVERTING_TO_PMTILES")
    await _run_export_job(
        data_object=geojson_file,
        check=check,
        step="pmtiles_export",
        remainder_types=["geojson", "pmtiles", "pmtiles-journal"],
        export_fn="to_pmtiles",
        job_keys=("pmtiles",),
    )


async def export_geojson_pmtiles(
    source: Table | CsvLike,
    check: dict,
) -> None:
    """RQ target: GeoJSON + PMTiles export for a db table."""
    resource_id = source.resource_id
    if resource_id:
        await ResourceJobStatus.set(resource_id, "geojson", "CONVERTING_TO_GEOJSON")
    geojson_file = await _run_export_job(
        data_object=source,
        check=check,
        delete_output=False,
        step="geojson_export",
        remainder_types=["geojson"],
        export_fn="to_geojson",
        job_keys=(),
    )
    if geojson_file is None:
        if resource_id:
            await ResourceJobStatus.clear(resource_id, "geojson")
        return
    if resource_id:
        await ResourceJobStatus.update(resource_id, "geojson", "pmtiles", "CONVERTING_TO_PMTILES")
    await _run_export_job(
        data_object=geojson_file,
        check=check,
        delete_input=True,
        step="pmtiles_export",
        remainder_types=["geojson", "pmtiles", "pmtiles-journal"],
        export_fn="to_pmtiles",
        job_keys=("pmtiles",),
    )

"""
Low-priority RQ jobs: exports from parsed DB tables (Parquet, GeoJSON + PMTiles).

Wrappers delegate to existing conversion helpers and own notify_udata,
ParseException handling, and resource status cleanup.
"""

from udata_hydra.analysis import helpers
from udata_hydra.conversion.db_to_geojson_and_pmtiles import db_to_geojson_and_pmtiles
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException, handle_parse_exception, remove_remainders


async def _run_export_job(
    table_name: str,
    inspection: dict,
    resource_id: str,
    check_id: int,
    url: str,
    step: str,
    remainder_types: list[str],
    export_fn,
) -> None:
    check_out = None
    try:
        await export_fn(table_name, inspection, resource_id, check_id)
    except Exception as e:
        remove_remainders(resource_id, remainder_types)
        check = await Check.get_by_id(check_id)
        try:
            raise ParseException(
                message=str(e),
                step=step,
                resource_id=resource_id,
                url=url,
                check_id=check_id,
            ) from e
        except ParseException as pe:
            check_out = await handle_parse_exception(pe, table_name, check)
    else:
        check_out = await Check.get_by_id(check_id)
    finally:
        resource = await Resource.get(resource_id)
        if resource is not None and check_out is not None:
            await helpers.notify_udata(resource, check_out)
        await Resource.update(resource_id, {"status": None})


async def export_parquet(
    table_name: str,
    inspection: dict,
    resource_id: str,
    check_id: int,
    url: str,
) -> None:
    """RQ target: parquet export for a parsed CSV resource."""
    from udata_hydra.analysis.csv import export_db_to_parquet

    await _run_export_job(
        table_name=table_name,
        inspection=inspection,
        resource_id=resource_id,
        check_id=check_id,
        url=url,
        step="parquet_export",
        remainder_types=["parquet"],
        export_fn=export_db_to_parquet,
    )


async def export_geojson_pmtiles(
    table_name: str,
    inspection: dict,
    resource_id: str,
    check_id: int,
    url: str,
) -> None:
    """RQ target: GeoJSON + PMTiles export for a parsed CSV resource."""
    await _run_export_job(
        table_name=table_name,
        inspection=inspection,
        resource_id=resource_id,
        check_id=check_id,
        url=url,
        step="geojson_export",
        remainder_types=["geojson", "pmtiles", "pmtiles-journal"],
        export_fn=db_to_geojson_and_pmtiles,
    )

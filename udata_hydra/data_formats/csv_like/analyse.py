import json
import logging
import os
from datetime import datetime, timezone

from asyncpg import Record

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.exports import export_geojson_pmtiles, export_parquet
from udata_hydra.data_formats import (
    Csv,
    Csvgz,
    CsvLike,
    Table,
    detect_data_format_from_check_or_catalog,
)
from udata_hydra.data_formats.csv_like.to_geojson import _detect_geo_columns
from udata_hydra.data_formats.table import Table
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
    queue,
)

log = logging.getLogger("udata-hydra")


async def analyse_csv(
    check: Record | dict,
    file: CsvLike | None = None,
    debug_insert: bool = False,
) -> None:
    """Launch csv analysis from a check or an URL (debug), using previously downloaded file if any"""
    if not config.CSV_ANALYSIS:
        log.debug("CSV_ANALYSIS turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])

    # Update resource status to ANALYSING_CSVLIKE
    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-csv", resource_id)
    assert any(_ is not None for _ in (check["id"], check["url"]))

    table, tmp_file = None, None
    try:
        if file is None:
            data_format = await detect_data_format_from_check_or_catalog(check)
            if data_format is None or not issubclass(data_format, CsvLike):
                # this should only happen when using this function directly (aka not after a resource check)
                raise TypeError("Could not infer the data format of the given file")
            tmp_file = await helpers.read_or_download_file(
                check=check,
                filename=None,
                data_format=data_format,
                exception=exception,
            )
            if data_format == Csvgz:
                # extraction has been done in read_or_download_file, now the file is a csv
                data_format = Csv
            file = data_format(
                path=tmp_file.name, resource_id=resource_id, dataset_id=check.get("dataset_id")
            )
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore[assignment]

        # Launch csv-detective against given file
        try:
            await file.inspect()
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="csv_detective",
                resource_id=resource_id,
                url=check["url"],
                check_id=check["id"],
            ) from e
        timer.mark("csv-inspection")

        if not config.CSV_TO_DB:
            log.debug(
                "CSV_TO_DB is off, skipping Postgres parsing table ingest and deferred export jobs."
            )
        else:
            table: Table = await file.to_db(
                check=check,
                table_indexes=table_indexes,
                debug_insert=debug_insert,
            )
            timer.mark("csv-to-db")

        check = await Check.update(  # type: ignore[assignment]
            check_id=check["id"],
            data={
                "parsing_finished_at": datetime.now(timezone.utc),
            },
        )

        if config.CSV_TO_DB:
            if (
                config.DB_TO_PARQUET
                and int(table.inspection.get("total_lines", 0)) >= config.MIN_LINES_FOR_PARQUET  # type: ignore[arg-type]
            ):
                queue.enqueue(
                    export_parquet,
                    table=table,
                    check=check,
                    _priority="low",
                    _exception=bool(exception),
                )

            if (
                config.DB_TO_GEOJSON
                and table is not None
                and _detect_geo_columns(table.inspection) is not None
            ):
                queue.enqueue(
                    export_geojson_pmtiles,
                    table=table,
                    check=check,
                    _priority="low",
                    _exception=bool(exception),
                )

    except (ParseException, IOException) as e:
        check = await handle_parse_exception(
            e, table.table_name if table is not None else None, check
        )  # type: ignore[assignment]
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})

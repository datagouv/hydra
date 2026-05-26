import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from asyncpg import Record
from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect
from csv_detective.detection.engine import engine_to_file

from udata_hydra import config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.tables_index import get_previous_analysis, insert_tables_index_entry
from udata_hydra.conversion.csv_to_db import csv_to_db
from udata_hydra.conversion.db_to_geojson_and_pmtiles import db_to_geojson_and_pmtiles
from udata_hydra.conversion.db_to_parquet import db_to_parquet
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    detect_tabular_from_headers,
    handle_parse_exception,
    remove_remainders,
)
from udata_hydra.utils.s3 import S3Client

log = logging.getLogger("udata-hydra")

_parquet_s3_client = S3Client(bucket=config.S3_PARQUET_BUCKET, prefix=config.S3_PARQUET_PREFIX)


async def analyse_csv(
    check: Record | dict,
    filename: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Launch csv analysis from a check or an URL (debug), using previously downloaded file if any"""
    if not config.CSV_ANALYSIS:
        log.debug("CSV_ANALYSIS turned off, skipping.")
        return

    # Preserve dataset_id from original check record
    dataset_id = check.get("dataset_id")

    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_CSV
    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)

    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-csv", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    table_name, tmp_file = None, None
    try:
        _, file_format = detect_tabular_from_headers(check)
        tmp_file = await helpers.read_or_download_file(
            check=check,
            filename=filename,
            file_format=file_format,
            exception=exception,
        )
        table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})  # type: ignore[assignment]

        # Launch csv-detective against given file
        try:
            previous_analysis: dict | None = await get_previous_analysis(resource_id=resource_id)
            if previous_analysis:
                await Resource.update(resource_id, {"status": "VALIDATING_CSV"})
                csv_inspection = validate_then_detect(
                    file_path=tmp_file.name,
                    previous_analysis=previous_analysis,
                    output_profile=True,
                    num_rows=-1,
                    save_results=False,
                )
            else:
                csv_inspection = csv_detective_routine(
                    file_path=tmp_file.name,
                    output_profile=True,
                    num_rows=-1,
                    save_results=False,
                )
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="csv_detective",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e
        timer.mark("csv-inspection")

        if not config.CSV_TO_DB:
            log.debug(
                "CSV_TO_DB is off, skipping Postgres parsing table ingest, GeoJSON/PMTiles export and Parquet export."
            )
        else:
            await csv_to_db(
                file_path=tmp_file.name,
                inspection=csv_inspection,
                table_name=table_name,
                table_indexes=table_indexes,
                resource_id=resource_id,
                debug_insert=debug_insert,
            )
            check = await Check.update(check["id"], {"parsing_table": table_name})  # type: ignore[assignment]
            timer.mark("csv-to-db")
            await insert_tables_index_entry(table_name, csv_inspection, check, dataset_id)

            try:
                if config.DB_TO_GEOJSON:
                    await db_to_geojson_and_pmtiles(
                        table_name=table_name,
                        inspection=csv_inspection,
                        resource_id=resource_id,
                        check_id=check["id"],
                        timer=timer,
                    )
                else:
                    log.debug("DB_TO_GEOJSON is off, skipping GeoJSON/PMTiles export.")
            except Exception as e:
                remove_remainders(resource_id, ["geojson", "pmtiles", "pmtiles-journal"])
                raise ParseException(
                    message=str(e),
                    step="geojson_export",
                    resource_id=resource_id,
                    url=url,
                    check_id=check["id"],
                ) from e

            if config.DB_TO_PARQUET:
                try:
                    await export_db_to_parquet(
                        table_name=table_name,
                        inspection=csv_inspection,
                        resource_id=resource_id,
                        check_id=check["id"],
                    )
                    timer.mark("csv-to-parquet")
                except Exception as e:
                    remove_remainders(resource_id, ["parquet"])
                    raise ParseException(
                        message=str(e),
                        step="parquet_export",
                        resource_id=resource_id,
                        url=url,
                        check_id=check["id"],
                    ) from e
            else:
                log.debug("DB_TO_PARQUET is off, skipping Parquet export.")

        check = await Check.update(  # type: ignore[assignment]
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
            },
        )

    except (ParseException, IOException) as e:
        check = await handle_parse_exception(e, table_name, check)  # type: ignore[assignment]
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


async def export_db_to_parquet(
    table_name: str,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
) -> tuple[str, int] | None:
    """
    Build a Parquet file from the parsing table, upload it to object storage,
    and store parquet_url / parquet_size on the check.

    This orchestrates storage and persistence. For writing only the Parquet
    file from the database table, see db_to_parquet.

    Requires a populated table from csv_to_db.
    """
    if int(inspection.get("total_lines", 0)) < config.MIN_LINES_FOR_PARQUET:
        log.debug(
            f"Skipping parquet export for {resource_id} because it has less than "
            f"{config.MIN_LINES_FOR_PARQUET} lines."
        )
        return None

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to parquet for {resource_id} and uploading to S3."
    )

    if resource_id:
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PARQUET"})

    parquet_file, _ = await db_to_parquet(
        table_name=table_name,
        inspection=inspection,
        output_filename=resource_id,
    )
    parquet_size: int = os.path.getsize(parquet_file)
    parquet_url: str = _parquet_s3_client.send_file(parquet_file)

    await Check.update(
        check_id,
        {
            "parquet_url": parquet_url,
            "parquet_size": parquet_size,
        },
    )
    return parquet_url, parquet_size

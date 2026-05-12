import logging
import os
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from csv_detective.detection.engine import engine_to_file

from udata_hydra import config
from udata_hydra.conversion.db_to_parquet import db_to_parquet
from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils.casting import generate_records
from udata_hydra.utils.minio import MinIOClient

log = logging.getLogger("udata-hydra")

minio_client = MinIOClient(bucket=config.MINIO_PARQUET_BUCKET, folder=config.MINIO_PARQUET_FOLDER)


def save_as_parquet(
    records: Iterator[list],
    columns: dict[str, dict],
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    # the "output_filename = None" case is only used in tests
    table = pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in records],
        schema=pa.schema(
            [pa.field(c, PYTHON_TYPE_TO_PA[columns[c]["python_type"]]) for c in columns]
        ),
    )
    if output_filename:
        pq.write_table(table, f"{output_filename}.parquet", compression="zstd")
    return f"{output_filename}.parquet", table


async def csv_to_parquet(
    file_path: str,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
    table_name: str | None = None,
) -> tuple[str, int] | None:
    """
    Convert a csv file to parquet using inspection data.

    If table_name is provided, reads directly from the PostgreSQL table
    instead of re-reading and re-casting the CSV file.

    Args:
        file_path: CSV file path to convert.
        inspection: CSV detective report.
        resource_id: used to name the parquet file.
        check_id: check ID to update with parquet info.
        table_name: if provided, read from this PostgreSQL table instead of file_path.

    Returns:
        parquet_url: URL of the parquet file.
        parquet_size: size of the parquet file.
    """
    if not config.CSV_TO_PARQUET and not config.DB_TO_PARQUET:
        log.debug("CSV_TO_PARQUET and DB_TO_PARQUET turned off, skipping parquet export.")
        return

    if int(inspection.get("total_lines", 0)) < config.MIN_LINES_FOR_PARQUET:
        log.debug(
            f"Skipping parquet export for {resource_id} because it has less than {config.MIN_LINES_FOR_PARQUET} lines."
        )
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to parquet for {resource_id} and sending to Minio."
    )

    if resource_id:
        # Update resource status to CONVERTING_TO_PARQUET
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PARQUET"})

    if config.DB_TO_PARQUET and table_name:
        parquet_file, _ = await db_to_parquet(
            table_name=table_name,
            inspection=inspection,
            output_filename=resource_id,
        )
    else:
        parquet_file, _ = save_as_parquet(
            records=generate_records(file_path, inspection, cast_json=False),
            columns=inspection["columns"],
            output_filename=resource_id,
        )
    parquet_size: int = os.path.getsize(parquet_file)
    parquet_url: str = minio_client.send_file(parquet_file)

    await Check.update(
        check_id,
        {
            "parquet_url": parquet_url,
            "parquet_size": parquet_size,
        },
    )
    # returning only for tests purposes
    return parquet_url, parquet_size

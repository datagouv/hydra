import hashlib
import logging
import uuid
from typing import TYPE_CHECKING

from csv_detective.detection.engine import engine_to_file
from progressist import ProgressBar

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.tables_index import insert_tables_index_entry
from udata_hydra.conversion.schema import compute_create_table_query
from udata_hydra.data_formats import CsvLike
from udata_hydra.db import compute_insert_query, db_col_name
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException
from udata_hydra.utils.casting import iter_tabular_rows

log = logging.getLogger("udata-hydra")
if TYPE_CHECKING:
    from udata_hydra.data_formats import Table


async def csv_to_db(
    file: CsvLike,
    check: dict,
    table_indexes: dict[str, str] | None = None,
    debug_insert: bool = False,
) -> "Table":
    """
    Convert a csv file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :df_chunks: chunks of pre-cast dataframes read from the file
    :inspection: CSV detective report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
    from udata_hydra.data_formats import Table

    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
    inspection: dict | None = file.inspection
    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to db for {table_name}"
    )

    if any(
        sum(len(char.encode("utf-8")) for char in col) > config.NAMEDATALEN - 1
        for col in inspection["columns"]
    ):
        raise ParseException(
            step="scan_column_names",
            resource_id=file.resource_id,
            table_name=table_name,
        ) from ValueError(
            f"Column names cannot exceed {config.NAMEDATALEN - 1} characters in Postgres"
        )

    if file.resource_id:
        # Update resource status to INSERTING_IN_DB
        await Resource.update(file.resource_id, {"status": "INSERTING_IN_DB"})

    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {db_col_name(c): helpers.get_python_type(v) for c, v in inspection["columns"].items()}

    shadow_table_name = f"{table_name}_s{uuid.uuid4().hex[:4]}"
    db = await context.pool("csv")

    step = None
    try:
        step = "create_table_query"
        async with db.acquire() as conn:
            async with conn.transaction():
                q_create = compute_create_table_query(
                    table_name=shadow_table_name, columns=columns, indexes=table_indexes
                )
                await conn.execute(q_create)

        step = "copy_records_to_table"
        async with db.acquire() as conn:
            if not debug_insert:
                await conn.copy_records_to_table(
                    shadow_table_name,
                    records=iter_tabular_rows(file.path, inspection, cast_json=False),
                    columns=list(columns.keys()),
                )
            else:
                bar = ProgressBar(total=inspection["total_lines"])
                for r in bar.iter(iter_tabular_rows(file.path, inspection, cast_json=False)):
                    data = {k: v for k, v in zip(inspection["columns"], r)}
                    q = compute_insert_query(
                        table_name=shadow_table_name, data=data, returning="__id"
                    )
                    await conn.execute(q, *data.values())

        step = "swap_tables"
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                await conn.execute(f'ALTER TABLE "{shadow_table_name}" RENAME TO "{table_name}"')
                rows = await conn.fetch(
                    "SELECT indexname FROM pg_indexes WHERE tablename = $1 AND schemaname = $2",
                    table_name,
                    config.DATABASE_SCHEMA,
                )
                for row in rows:
                    if row["indexname"].startswith(shadow_table_name):
                        new_name = f"{table_name}{row['indexname'][len(shadow_table_name) :]}"
                        if new_name != row["indexname"]:
                            q = f'"{config.DATABASE_SCHEMA}"."{row["indexname"]}"'
                            await conn.execute(f'ALTER INDEX IF EXISTS {q} RENAME TO "{new_name}"')
    except Exception as e:
        try:
            async with db.acquire() as conn:
                await conn.execute(f'DROP TABLE IF EXISTS "{shadow_table_name}"')
        except Exception:
            log.warning("Failed to clean up shadow table %s", shadow_table_name)
        raise ParseException(
            message=str(e),
            step=step,
            resource_id=file.resource_id,
            table_name=table_name,
        ) from e

    check = await Check.update(check["id"], {"parsing_table": table_name})  # type: ignore[assignment]
    await insert_tables_index_entry(table_name, file.inspection, check, file.dataset_id)
    return Table(
        table_name=table_name,
        inspection=file.inspection,
        resource_id=file.resource_id,
        dataset_id=file.dataset_id,
    )

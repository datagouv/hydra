import logging

from csv_detective.detection.engine import engine_to_file
from progressist import ProgressBar

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.conversion.schema import compute_create_table_query
from udata_hydra.db import compute_insert_query, db_col_name
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException
from udata_hydra.utils.casting import iter_tabular_rows

log = logging.getLogger("udata-hydra")


async def csv_to_db(
    file_path: str,
    inspection: dict,
    table_name: str,
    table_indexes: dict[str, str] | None = None,
    resource_id: str | None = None,
    debug_insert: bool = False,
) -> None:
    """
    Convert a csv file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :df_chunks: chunks of pre-cast dataframes read from the file
    :inspection: CSV detective report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
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
            resource_id=resource_id,
            table_name=table_name,
        ) from ValueError(
            f"Column names cannot exceed {config.NAMEDATALEN - 1} characters in Postgres"
        )

    if resource_id:
        # Update resource status to INSERTING_IN_DB
        await Resource.set_job_status(resource_id, "csv", "INSERTING_IN_DB")

    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {db_col_name(c): helpers.get_python_type(v) for c, v in inspection["columns"].items()}

    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)

    # Create table
    q = compute_create_table_query(table_name=table_name, columns=columns, indexes=table_indexes)
    try:
        await db.execute(q)
    except Exception as e:
        raise ParseException(
            message=str(e),
            step="create_table_query",
            resource_id=resource_id,
            table_name=table_name,
        ) from e

    # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
    if not debug_insert:
        # NB: also see copy_to_table for a file source
        try:
            await db.copy_records_to_table(
                table_name,
                records=iter_tabular_rows(file_path, inspection, cast_json=False),
                columns=list(columns.keys()),
            )
        except Exception as e:  # I know what I'm doing, pinky swear
            raise ParseException(
                message=str(e),
                step="copy_records_to_table",
                resource_id=resource_id,
                table_name=table_name,
            ) from e
    # this inserts rows from iterator one by one, slow but useful for debugging
    else:
        bar = ProgressBar(total=inspection["total_lines"])
        for r in bar.iter(iter_tabular_rows(file_path, inspection, cast_json=False)):
            data = {k: v for k, v in zip(inspection["columns"], r)}
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())

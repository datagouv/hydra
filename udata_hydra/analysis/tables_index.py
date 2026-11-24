import json

from asyncpg import Record
from csv_detective.utils import sanitize_for_json

from udata_hydra import context
from udata_hydra.utils import ParseException


async def get_previous_inspection(resource_id: str) -> dict | None:
    db = await context.pool("csv")
    q = (
        "SELECT parsing_table, csv_detective FROM tables_index "
        f"WHERE resource_id='{resource_id}' AND deleted_at IS NULL "
        "ORDER BY created_at DESC LIMIT 1"
    )
    res = await db.fetch(q)
    if not res:
        return None
    inspection = json.loads(res[0]["csv_detective"])
    # the csv_detective column is JSONB: it reorders the keys of `columns`,
    # but keeps the order of the `header` array, which is the order of the file
    if set(inspection["header"]) != set(inspection["columns"]):
        # older csv-detective versions read the header from the raw text, not from the parsed table
        return None
    inspection["columns"] = {col: inspection["columns"][col] for col in inspection["header"]}
    return inspection


async def insert_tables_index_entry(
    table_name: str, inspection: dict, check: Record | dict, dataset_id: str | None
) -> None:
    """Insert metadata for a parsed table into `DATABASE_URL_CSV.tables_index`."""
    db = await context.pool("csv")
    q = "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, dataset_id, url) VALUES($1, $2, $3, $4, $5)"
    try:
        await db.execute(
            q,
            table_name,
            json.dumps(sanitize_for_json(inspection), default=str),
            check.get("resource_id"),
            dataset_id,
            check.get("url"),
        )
    except Exception as e:
        raise ParseException(
            message=str(e),
            step="tables_index_insertion",
            resource_id=check.get("resource_id"),
            url=check.get("url"),
            check_id=check["id"],
        ) from e

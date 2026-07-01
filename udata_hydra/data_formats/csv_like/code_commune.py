import logging

from udata_hydra import config, context
from udata_hydra.data_formats.table import Table
from udata_hydra.db import db_col_name

log = logging.getLogger("udata-hydra")


def _detect_code_commune_columns(inspection: dict) -> dict[str, str]:
    """Map original column name -> postgres column name, for every column
    csv_detective tagged as format == "code_commune"."""
    return {
        column: db_col_name(column)
        for column, detection in inspection["columns"].items()
        if detection.get("format") == "code_commune"
    }


async def extract_code_commune_values(table: Table) -> dict[str, list[str]] | None:
    """Query the just-populated parsing table for distinct values of every
    code_commune column. Returns None if there are no such columns, or if
    none of them yielded a usable (within-cap) result."""
    if not config.CODE_COMMUNE_ANALYSIS_ENABLED:
        return None

    columns = _detect_code_commune_columns(table.inspection)
    if not columns:
        return None

    db = await context.pool("csv")
    result: dict[str, list[str]] = {}
    cap = config.MAX_CODE_COMMUNE_VALUES

    for original_col, db_col in columns.items():
        q = f'''
            SELECT DISTINCT "{db_col}" AS value
            FROM "{table.table_name}"
            WHERE "{db_col}" IS NOT NULL
            LIMIT {cap + 1}
        '''
        rows = await db.fetch(q)
        values = [r["value"] for r in rows]
        if len(values) > cap:
            log.warning(
                f"code_commune column '{original_col}' on table {table.table_name} has more "
                f"than {cap} distinct values, skipping (likely a bad detection)"
            )
            continue
        if values:
            result[original_col] = sorted(str(v) for v in values)

    return result or None

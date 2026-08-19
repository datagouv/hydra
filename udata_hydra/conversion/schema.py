import logging

import pyarrow as pa
from slugify import slugify
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateIndex, CreateTable, Index

from udata_hydra import config

log = logging.getLogger("udata-hydra")

PYTHON_TYPE_TO_PG = {
    "string": String,
    "float": Float,
    "int": BigInteger,
    "bool": Boolean,
    "json": JSON,
    "date": Date,
    "datetime": DateTime,
    "datetime_aware": DateTime(timezone=True),
    "binary": LargeBinary,
}

PYARROW_TYPE_TO_PYTHON = {
    # using regex because of bits-differing types (e.g. int32 and int64)
    # the "^" makes sure we don't consider the types of elements within structured objects (lists, dicts)
    "string$": "string",  # large_string also exists
    "^double": "float",
    "^float": "float",
    "^decimal": "float",
    "^int": "int",
    "^bool": "bool",
    "^date": "date",
    "^struct": "json",  # dictionary
    "^list": "json",
    "^binary": "binary",
    r"^timestamp\[\ws\]": "datetime",
    r"^timestamp\[\ws,": "datetime_aware",  # the rest of the field depends on the timezone
}

PYTHON_TYPE_TO_PA = {
    "string": pa.string(),
    "float": pa.float64(),
    "int": pa.int64(),
    "bool": pa.bool_(),
    "json": pa.string(),
    "date": pa.date32(),
    "datetime": pa.date64(),
    "binary": pa.binary(),
}


def compute_create_table_query(
    table_name: str, columns: dict, indexes: dict[str, str] | None = None
) -> str:
    """Use sqlalchemy to build a CREATE TABLE statement that should not be vulnerable to injections"""

    metadata = MetaData()
    table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))

    for col_name, col_type in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_type, String)))

    indexes_labels = {}
    if indexes:
        for col_name, index_type in indexes.items():
            if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                log.error(
                    f'Index type "{index_type}" is unknown or not supported yet! '
                    f"Index for column {col_name} was not created."
                )
                continue

            else:
                if index_type == "index":
                    index_name = f"{table_name}_{slugify(col_name)}_idx"
                    indexes_labels[index_name] = {"column": col_name, "type": index_type}
                    try:
                        table.append_constraint(Index(index_name, col_name))
                    except KeyError:
                        raise KeyError(
                            f'Error creating index "{index_name}" on column "{col_name}". '
                            f'Does the column "{col_name}" exist in the table?'
                        )
                # TODO: other index types. Not easy with sqlalchemy, maybe use raw sql?

    compiled_query = CreateTable(table).compile(dialect=asyncpg.dialect())
    query: str = compiled_query.string

    # Add the index creation queries to the main query
    for index in table.indexes:
        log.debug(
            f'Creating {indexes_labels[index.name]["type"]} on column "{indexes_labels[index.name]["column"]}"'
        )
        query_idx = CreateIndex(index).compile(dialect=asyncpg.dialect())
        query: str = query + ";" + query_idx.string

    # compiled query will want to write "%% mon pourcent" VARCHAR but will fail when querying "% mon pourcent"
    # also, "% mon pourcent" works well in pg as a column
    # TODO: dirty hack, maybe find an alternative
    query = query.replace("%%", "%")
    return query

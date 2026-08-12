import hashlib
import re

import pytest
from sqlalchemy.exc import ConstraintColumnNotFoundError

from udata_hydra.conversion.schema import compute_create_table_query
from udata_hydra.db import PG_MAX_IDENTIFIER_BYTES

TABLE_NAME = hashlib.md5(b"http://example.com/csv").hexdigest()


def index_names(query: str) -> list[str]:
    return re.findall(r'CREATE INDEX "([^"]+)"', query)


def test_index_name_fits_in_the_identifier_limit():
    # the table name alone is 32 characters, so a verbose column overflows quickly
    col = "Nombre de logements sociaux conventionnes livres au cours de l'annee"
    query = compute_create_table_query(TABLE_NAME, {col: "string"}, indexes={col: "index"})
    (name,) = index_names(query)
    assert len(name.encode("utf-8")) <= PG_MAX_IDENTIFIER_BYTES


def test_index_names_of_columns_sharing_a_prefix_are_distinct():
    prefix = "Nombre de logements sociaux conventionnes livres au cours de l'annee "
    first, second = prefix + "2023", prefix + "2024"
    query = compute_create_table_query(
        TABLE_NAME,
        {first: "string", second: "string"},
        indexes={first: "index", second: "index"},
    )
    assert len(set(index_names(query))) == 2


def test_index_names_of_columns_slugify_cannot_transliterate():
    # slugify("🐟🐟") is an empty string, so both indexes would share the same name
    first, second = "🐟🐟", "..."
    query = compute_create_table_query(
        TABLE_NAME,
        {first: "string", second: "string"},
        indexes={first: "index", second: "index"},
    )
    assert len(set(index_names(query))) == 2


def test_index_on_an_unknown_column_raises():
    with pytest.raises(ConstraintColumnNotFoundError, match="absente"):
        compute_create_table_query(TABLE_NAME, {"a": "int"}, indexes={"absente": "index"})


def test_unsupported_index_type_is_skipped():
    query = compute_create_table_query(TABLE_NAME, {"a": "int"}, indexes={"a": "gist"})
    assert index_names(query) == []

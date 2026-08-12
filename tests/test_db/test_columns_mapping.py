from unittest.mock import patch

import pytest

from udata_hydra.db import build_db_columns_mapping, truncate_utf8

# Postgres' actual limit, i.e. the default config.NAMEDATALEN - 1
LIMIT = 63


def db_names(columns: list[str]) -> list[str]:
    return list(build_db_columns_mapping(columns).values())


def test_short_ascii_names_are_untouched():
    columns = ["id", "name", "% mon pourcent", "l'adresse"]
    assert build_db_columns_mapping(columns) == {c: c for c in columns}


def test_name_at_the_exact_limit_is_kept():
    col = "a" * LIMIT
    assert build_db_columns_mapping([col]) == {col: col}


def test_name_one_byte_over_the_limit_is_truncated():
    col = "a" * (LIMIT + 1)
    (db_col,) = db_names([col])
    assert db_col == "a" * (LIMIT - len("__col0")) + "__col0"
    assert len(db_col.encode("utf-8")) == LIMIT


def test_truncation_counts_bytes_not_characters():
    # 32 euro signs are 96 bytes but only 32 characters
    col = "€" * 32
    (db_col,) = db_names([col])
    assert len(db_col.encode("utf-8")) <= LIMIT
    assert len(col.encode("utf-8")) > LIMIT


@pytest.mark.parametrize("char", ("é", "€", "🐟"))
def test_truncation_cuts_on_a_character_boundary(char):
    col = char * 40
    (db_col,) = db_names([col])
    assert len(db_col.encode("utf-8")) <= LIMIT
    assert db_col.endswith("__col0")
    prefix = db_col[: -len("__col0")]
    # every kept character is whole, and they are the first ones of the source name
    assert prefix == char * len(prefix)
    assert col.startswith(prefix)


def test_truncate_utf8_never_returns_a_replacement_character():
    assert truncate_utf8("é" * 10, 5) == "éé"
    assert "�" not in truncate_utf8("🐟" * 10, 7)


def test_reserved_column_is_renamed():
    assert build_db_columns_mapping(["xmin", "XMax"]) == {
        "xmin": "xmin__hydra_renamed",
        "XMax": "XMax__hydra_renamed",
    }


def test_reserved_column_rename_respects_the_limit():
    # the __hydra_renamed suffix is 15 bytes: it must fit in the budget too
    with patch("udata_hydra.config.NAMEDATALEN", 10):
        assert build_db_columns_mapping(["xmin"]) == {"xmin": "xmi__col0"}


def test_reserved_column_at_the_limit_is_truncated():
    col = "xmin" + "a" * (LIMIT - len("xmin__hydra_renamed"))
    # the source name fits, but the renamed one doesn't
    assert len(col.encode("utf-8")) <= LIMIT
    (db_col,) = db_names([col])
    assert len(db_col.encode("utf-8")) <= LIMIT


def test_long_columns_sharing_a_prefix_get_distinct_names():
    prefix = "Nombre de logements sociaux conventionnés livrés au cours de l'année "
    columns = [prefix + "2023", prefix + "2024"]
    first, second = db_names(columns)
    assert first != second
    assert first.endswith("__col0")
    assert second.endswith("__col1")


def test_positional_index_is_the_column_position():
    columns = ["a", "b", "c" * (LIMIT + 1)]
    assert db_names(columns)[2].endswith("__col2")


def test_short_column_colliding_with_a_truncated_name():
    long_col = "a" * (LIMIT + 1)
    (truncated,) = db_names([long_col])
    mapping = build_db_columns_mapping([long_col, truncated])
    assert len(set(mapping.values())) == 2
    assert all(len(name.encode("utf-8")) <= LIMIT for name in mapping.values())


def test_duplicate_reserved_renaming_does_not_collide():
    # a file having both `xmin` and `xmin__hydra_renamed` used to break the CREATE TABLE
    mapping = build_db_columns_mapping(["xmin", "xmin__hydra_renamed"])
    assert len(set(mapping.values())) == 2


def test_order_is_preserved():
    columns = ["c", "a", "b" * (LIMIT + 1)]
    assert list(build_db_columns_mapping(columns)) == columns


def test_raises_when_the_budget_is_too_small_for_a_suffix():
    with patch("udata_hydra.config.NAMEDATALEN", 7):
        with pytest.raises(ValueError, match="too small"):
            build_db_columns_mapping(["a" * 20])

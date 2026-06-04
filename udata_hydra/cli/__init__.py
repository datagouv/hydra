"""Hydra Typer CLI: command modules register on `cli` at import time."""

import logging
import sys

from udata_hydra.cli import analysis, catalog, crawl, db, purge  # noqa: F401
from udata_hydra.cli.analysis import analyse_csv_cli
from udata_hydra.cli.catalog import (
    insert_resource_into_catalog,
    insert_url_into_catalog,
    load_catalog,
)
from udata_hydra.cli.common import _find_check, _make_async_wrapper, cli, connection, context, log
from udata_hydra.cli.crawl import probe_cors_cli
from udata_hydra.cli.db import drop_dbs, migrate
from udata_hydra.cli.purge import purge_checks, purge_csv_tables, purge_selected_csv_tables
from udata_hydra.crawl.check_resources import probe_cors

__all__ = [
    "analyse_csv_cli",
    "cli",
    "connection",
    "context",
    "drop_dbs",
    "_find_check",
    "insert_resource_into_catalog",
    "insert_url_into_catalog",
    "load_catalog",
    "log",
    "_make_async_wrapper",
    "migrate",
    "probe_cors",
    "probe_cors_cli",
    "purge_checks",
    "purge_csv_tables",
    "purge_selected_csv_tables",
    "run",
]


def run() -> None:
    """CLI entry point: silence logs below ERROR when --quiet is in argv, before loading commands."""
    if "--quiet" in sys.argv:
        logging.disable(logging.WARNING)
    cli()


if __name__ == "__main__":
    run()

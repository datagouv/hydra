"""CLI entry point: apply --quiet before loading cli.py (and its heavy imports)."""

import logging
import sys


def run() -> None:
    """CLI entry point: silence logs below ERROR when --quiet is in argv, before importing cli."""
    if "--quiet" in sys.argv:
        logging.disable(logging.WARNING)
    from udata_hydra.cli import cli

    cli()

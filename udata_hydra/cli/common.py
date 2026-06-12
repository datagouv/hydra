import asyncio
from asyncio import run as aiorun

import asyncpg
import typer

from udata_hydra import config
from udata_hydra.db.check import Check
from udata_hydra.logger import setup_logging

cli = typer.Typer()
context: dict = {"conn": {}}
log = setup_logging()


async def connection(db_name: str = "main"):
    if db_name not in context["conn"]:
        dsn = (
            config.DATABASE_URL
            if db_name == "main"
            else getattr(config, f"DATABASE_URL_{db_name.upper()}")
        )
        context["conn"][db_name] = await asyncpg.connect(
            dsn=dsn, server_settings={"search_path": config.DATABASE_SCHEMA}
        )
    return context["conn"][db_name]


def _make_async_wrapper(async_func):
    """Create a wrapper that works both in sync and async contexts."""

    def wrapper(*args, **kwargs):
        try:
            asyncio.get_running_loop()
            return async_func(*args, **kwargs)
        except RuntimeError:
            return aiorun(async_func(*args, **kwargs))

    return wrapper


async def _find_check(
    check_id: str | None = None,
    url: str | None = None,
    resource_id: str | None = None,
) -> dict | None:
    """Look up an existing check by check_id, url or resource_id."""
    assert check_id or url or resource_id

    check = None

    if check_id:
        record = await Check.get_by_id(int(check_id), with_deleted=True)
        check = dict(record) if record else None

    if not check and url:
        records = await Check.get_by_url(url)
        if records:
            if len(records) > 1:
                log.warning(f"Multiple checks found for URL {url}, using the latest one")
            check = dict(records[0])

    if not check and resource_id:
        record = await Check.get_by_resource_id(resource_id)
        check = dict(record) if record else None

    if not check:
        if check_id:
            log.error("Could not retrieve the specified check")
        elif url:
            log.error("Could not find a check linked to the specified URL")
        elif resource_id:
            log.error("Could not find a check linked to the specified resource ID")

    return check

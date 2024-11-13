import logging
from datetime import datetime, timezone
from tkinter import N

import sentry_sdk
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.check import Check

log = logging.getLogger("udata-hydra")


class ParseException(Exception):
    """
    Exception raised when an error occurs during parsing.
    Enriches Sentry with tags if available.
    """

    def __init__(
        self,
        step: str | None = None,
        resource_id: str | None = None,
        url: str | None = None,
        check_id: int | None = None,
        table_name: str | None = None,
        *args,
    ) -> None:
        if step:
            self.step = step
        if config.SENTRY_DSN:
            with sentry_sdk.new_scope() as scope:
                # scope.set_level("warning")
                scope.set_tags(
                    {
                        "resource_id": resource_id or "unknown",
                        "csv_url": url or "unknown",
                        "check_id": check_id or "unknown",
                        "table_name": table_name or "unknown",
                    }
                )
        super().__init__(*args)


async def handle_parse_exception(e: ParseException, table_name: str, check: Record | None) -> None:
    """Specific ParsingError handling. Store error if in a check context. Also cleanup :table_name: if needed."""
    db = await context.pool("csv")
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    if check:
        if config.SENTRY_DSN:
            with sentry_sdk.new_scope():
                event_id = sentry_sdk.capture_exception(e)
        # e.__cause__ let us access the "inherited" error of ParseException (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        err = f"{e.step}:sentry:{event_id}" if config.SENTRY_DSN else f"{e.step}:{str(e.__cause__)}"
        await Check.update(
            check["id"],
            {"parsing_error": err, "parsing_finished_at": datetime.now(timezone.utc)},
        )
        log.error("Parsing error", exc_info=e)
    else:
        raise e

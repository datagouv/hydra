import logging
from datetime import datetime, timezone

import sentry_sdk
from asyncpg import Record

from udata_hydra import context
from udata_hydra.db.check import Check

log = logging.getLogger("udata-hydra")


class ExceptionWithSentryDetails(Exception):
    """
    Custom exception that automatically captures to Sentry with enriched tags when
    raised.

    This implementation captures the exception in the __str__ method, ensuring the
    full stack trace is available.

    The exception automatically attaches resource ID, URL, check ID, and other
    context to Sentry events. Tags and exception capture happen in the same scope,
    ensuring all the custom tags (resource details, step information, etc.) are
    preserved.

    Two previous approaches failed:
    - Capturing in __init__: No stack trace is available, because __init__ is
      running when the exception is created, while __str__ runs when it's raised
      and converted to a string, which provides the complete call stack.
    - Separate scopes: Tags were set in a temporary scope that got destroyed
      before exception capture, resulting in Sentry events without the custom
      tags. Later, when trying to capture the exception, those tags were no
      longer available.

    Usage:
        raise ExceptionWithSentryDetails(
            message="Something went wrong",
            step="data_processing",
            resource_id="123",
            url="https://example.com/data.csv",
            check_id=456,
            table_name="processed_data"
        )
    """

    def __init__(
        self,
        message: str | None = None,
        step: str | None = None,
        resource_id: str | None = None,
        url: str | None = None,
        check_id: int | None = None,
        table_name: str | None = None,
        *args,
    ) -> None:
        self.step = step
        self.message = message
        self.resource_id = resource_id
        self.url = url
        self.check_id = check_id
        self.table_name = table_name
        self.sentry_event_id = None
        # NO auto-capture here - let the exception be raised normally
        super().__init__(message, *args)

    def __str__(self):
        """
        Capture to Sentry when the exception is converted to string.
        This happens when it's raised and caught, ensuring full stack trace.
        """
        # Only capture if we have a traceback (exception is being raised)
        if getattr(self, "__traceback__", None) is not None:
            if sentry_sdk.Hub.current.client:
                with sentry_sdk.new_scope() as scope:
                    scope.set_tags(
                        {
                            "resource_id": self.resource_id or "unknown",
                            "url": self.url or "unknown",
                            "check_id": self.check_id or "unknown",
                            "table_name": self.table_name or "unknown",
                            "step": self.step or "unknown",
                            "exception_type": self.__class__.__name__,
                        }
                    )

                    event_id = sentry_sdk.capture_exception(self)
                    self.sentry_event_id = event_id  # Store the event ID, so it can be used by handle_parse_exception to be stored in check

                    # # If we want also to display the chained exception performance stack trace we could check for chained exceptions (__cause__ or __context__)
                    # # But we would have both original and chained exception appear in Sentry
                    # original_exception = getattr(self, "__cause__", None) or getattr(
                    #     self, "__context__", None
                    # )
                    # if original_exception:
                    #     # Capture the ORIGINAL exception with its full stack trace
                    #     sentry_sdk.capture_exception(original_exception)

        return super().__str__()


class ParseException(ExceptionWithSentryDetails):
    """Exception raised when an error occurs during parsing."""

    pass


class IOException(ExceptionWithSentryDetails):
    """Exception raised when an error occurs during IO operations."""

    pass


async def handle_parse_exception(
    e: IOException | ParseException, table_name: str | None, check: Record | None
) -> None:
    """Specific IO/ParseException handling. Store error in :check: if in a check context. Also cleanup :table_name: if needed."""
    if table_name is not None:
        db = await context.pool("csv")
        await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        await db.execute(f"DELETE FROM tables_index WHERE parsing_table='{table_name}'")
    if check:
        # e.__cause__ let us access the "inherited" error of the Exception (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        err = f"{e.step}:{str(e.__cause__)}"
        if e.sentry_event_id:
            err = f"{e.step}:sentry:{e.sentry_event_id}"
        await Check.update(
            check["id"],
            {"parsing_error": err, "parsing_finished_at": datetime.now(timezone.utc)},
        )
        log.error("Parsing error", exc_info=e)
    else:
        raise e

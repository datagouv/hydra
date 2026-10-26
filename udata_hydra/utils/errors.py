import logging
from datetime import datetime, timezone

import sentry_sdk
from asyncpg import Record

from udata_hydra import context
from udata_hydra.db.check import Check

log = logging.getLogger("udata-hydra")


# Allowlist of well-known data/value parsing errors that may be republished in
# `analysis:parsing:error`. Everything else is redacted: it may echo DB text or
# leak infrastructure details. Deliberate exceptions: OverflowError stays redacted
# (ambiguous origin), while ValueError is exposed even though owslib OGC errors
# (embedding endpoint URLs) also raise it — a known, accepted leak.
_SAFE_EXPOSE_TYPES = (
    ValueError,  # also covers UnicodeError (UnicodeDecodeError/UnicodeEncodeError)
    TypeError,
    KeyError,
    IndexError,
)


def _is_safe_to_expose(cause: BaseException | None) -> bool:
    """Whether `cause` may be republished in `analysis:parsing:error`.

    Allowlist: only well-known data/value parsing errors are exposed verbatim.
    Everything else is redacted to `error`, as it may echo attacker-controlled DB
    text or leak infrastructure details (host, port, endpoint, bucket).
    """
    if cause is None:
        # No type-checkable cause: our free-form message (which may embed server
        # paths) is never republished; it is still logged server-side.
        return False
    if isinstance(cause, OSError):
        # Some infra errors (ssl, requests, aiohttp connectors) inherit ValueError.
        return False
    if type(cause).__module__.startswith("asyncpg"):
        # Client-side asyncpg errors inherit ValueError but embed connection settings.
        return False
    return isinstance(cause, _SAFE_EXPOSE_TYPES)


class ExceptionWithSentryDetails(Exception):
    """
    Custom exception that automatically captures to Sentry with enriched tags when
    raised.

    This implementation captures the exception in the __str__ method, ensuring the
    full stack trace is available. Set report_to_sentry to False on a subclass to
    store the message with str() without reporting.

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

    report_to_sentry = True

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
        if self.report_to_sentry and getattr(self, "__traceback__", None) is not None:
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
                    # handle_parse_exception only persists this id for cause-less errors: there it
                    # calls str(e) before Check.update, triggering this capture. For cause-bearing
                    # errors str(e) runs later (in log.error), so the id is not stored.
                    self.sentry_event_id = event_id

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


class FileTooLargeException(IOException):
    """Expected size limit. Not reported to Sentry."""

    report_to_sentry = False


async def handle_parse_exception(
    e: IOException | ParseException, table_name: str | None, check: Record | dict | None
) -> Record | None:
    """Specific IO/ParseException handling. Store error in :check: if in a check context. Also cleanup :table_name: if needed."""
    if table_name is not None and (check and not check.get("parsing_table")):
        # only deleting the table if we have not successfully completed csv_to_db
        db = await context.pool("csv")
        await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        await db.execute(f"DELETE FROM tables_index WHERE parsing_table='{table_name}'")
    if check:
        # e.__cause__ let us access the "inherited" error of the Exception (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        # We check if the cause is safe to be exposed and stored in `analysis:parsing:error`.
        # The details are logged server-side in any case.
        cause = e.__cause__
        detail = f"{e.step}:{cause}" if cause is not None else str(e)
        err = detail if _is_safe_to_expose(cause) else f"{e.step}:error"
        if e.sentry_event_id:
            err = f"{e.step}:sentry:{e.sentry_event_id}"
        check_id = check["id"]
        check = await Check.update(
            check_id,
            {"parsing_error": err, "parsing_finished_at": datetime.now(timezone.utc)},
        )
        log.error(
            "Parsing error for check %s at step %s: %s",
            check_id,
            e.step,
            detail,
            exc_info=e,
        )
        return check
    else:
        raise e

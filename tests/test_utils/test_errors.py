"""Tests for exception handling in `udata_hydra.utils.errors`.

Covers:
- `ExceptionWithSentryDetails` (Sentry capture is triggered lazily on `str()`).
- the `_is_safe_to_expose` allowlist used to redact `analysis:parsing:error`.
- end-to-end `handle_parse_exception` redaction against the test DB, using the
  real exception types and messages observed in the `analysis:parsing:error`
  values of the production resources export.
"""

import ssl

import aiohttp
import asyncpg
import pytest
import requests.exceptions
from aiohttp.client_reqrep import RequestInfo
from yarl import URL

from udata_hydra.utils.errors import (
    ParseException,
    _is_safe_to_expose,
    handle_parse_exception,
)


def _test_function_1():
    """Function that will raise an exception"""
    return _test_function_2()


def _test_function_2():
    """Intermediate function"""
    return _test_function_3()


def _test_function_3():
    """Function that raises the exception"""
    raise ValueError("Test exception for stack trace")


def test_sentry_auto_with_raise_from():
    """Test that ParseException automatically retrieves original exception when raised with 'from e'."""
    try:
        _test_function_1()
    except Exception as e:
        try:
            # Use 'raise ... from e' to set __cause__
            raise ParseException(
                message=str(e),
                step="test_step",
                resource_id="test_resource_123",
                url="https://example.com/test",
                check_id=456,
            ) from e
        except ParseException as parse_exception:
            # Now __cause__ should be defined
            assert parse_exception.__cause__ is not None
            assert parse_exception.__cause__ == e
            assert isinstance(parse_exception.__cause__, ValueError)
            assert str(parse_exception.__cause__) == "Test exception for stack trace"

            # Verify that the traceback is preserved
            assert parse_exception.__cause__.__traceback__ is not None

            # Simulate Sentry logic
            original_exception = getattr(parse_exception, "__cause__", None) or getattr(
                parse_exception, "__context__", None
            )

            assert original_exception is not None
            assert original_exception == e
            assert original_exception.__traceback__ is not None


def test_is_safe_to_expose_value_error():
    """Deterministic, producer-facing messages (ValueError and friends) may be
    republished: they carry no attacker-controlled or DB-internal text."""
    assert _is_safe_to_expose(ValueError("Number of columns is not even")) is True
    assert _is_safe_to_expose(ValueError("Column names cannot exceed 63 characters")) is True
    # Common data-casting errors observed in production are allowed.
    assert _is_safe_to_expose(ValueError("invalid literal for int() with base 10: '0.0'")) is True
    assert (
        _is_safe_to_expose(TypeError("can't subtract offset-naive and offset-aware datetimes"))
        is True
    )
    assert _is_safe_to_expose(IndexError("list index out of range")) is True
    assert _is_safe_to_expose(KeyError("header_row_idx")) is True
    # UnicodeDecodeError is a subclass of ValueError (via UnicodeError).
    assert (
        _is_safe_to_expose(UnicodeDecodeError("utf-8", b"\x8b", 0, 1, "invalid start byte")) is True
    )
    # A ParseException without an underlying cause has no type-checkable message:
    # our own free-form message is never republished.
    assert _is_safe_to_expose(None) is False


def test_is_safe_to_expose_postgres_error():
    """Raw PostgreSQL errors must never be republished verbatim: a hostile CSV
    column name can be echoed back (e.g. 'invalid input syntax for type integer:
    "demo-hydra"', 'syntax error at or near "1.0"'). These carry either
    attacker-influenced text or database internals."""
    assert (
        _is_safe_to_expose(asyncpg.InvalidTextRepresentationError("invalid input syntax")) is False
    )
    assert (
        _is_safe_to_expose(asyncpg.UndefinedFunctionError("function x() does not exist")) is False
    )
    assert (
        _is_safe_to_expose(asyncpg.UndefinedColumnError('column "title" does not exist')) is False
    )
    # Connection errors also inherit from PostgresError.
    assert _is_safe_to_expose(asyncpg.PostgresConnectionError("connection refused")) is False
    # A generic subclass is covered too.
    assert _is_safe_to_expose(asyncpg.PostgresError("some pg error")) is False


def test_is_safe_to_expose_os_and_http_errors():
    """Infrastructure errors must NOT leak host/port/endpoint/bucket details to
    producers, even though they are not PostgreSQL errors. This is why the check
    is an allowlist and not a deny-list."""
    # OSError and its subclasses (TimeoutError, ConnectionError) may carry host/port.
    assert _is_safe_to_expose(OSError("connection refused to 10.0.0.1:5432")) is False
    assert _is_safe_to_expose(TimeoutError("timed out connecting to db.internal:5432")) is False
    assert (
        _is_safe_to_expose(ConnectionError("could not connect to s3.rbx.io.cloud.ovh.net")) is False
    )
    # HTTP client errors (e.g. from aiohttp) may embed URLs/status.
    assert (
        _is_safe_to_expose(aiohttp.ClientResponseError(None, None, status=500, message="boom"))
        is False
    )


def test_is_safe_to_expose_infra_errors_inheriting_value_error():
    """Some infrastructure errors also inherit ValueError and would otherwise
    match the allowlist: SSL/connector errors embed host names and IPs, asyncpg
    client errors embed connection settings."""
    ssl_error = ssl.SSLCertVerificationError(1, "certificate is not valid for '10.0.0.5'")
    assert isinstance(ssl_error, ValueError)
    assert _is_safe_to_expose(ssl_error) is False

    connector_error = aiohttp.ClientConnectorCertificateError(None, ssl_error)
    assert _is_safe_to_expose(connector_error) is False

    assert _is_safe_to_expose(requests.exceptions.InvalidURL("http://[bad")) is False

    asyncpg_error = asyncpg.exceptions.ClientConfigurationError(
        "failed to connect to host 'db.internal' port 5432"
    )
    assert isinstance(asyncpg_error, ValueError)
    assert _is_safe_to_expose(asyncpg_error) is False


def _http_error():
    info = RequestInfo(
        url=URL("http://example.com/data.csv"),
        method="GET",
        headers={},
        real_url=URL("http://example.com/data.csv"),
    )
    return aiohttp.ClientResponseError(info, (), status=500, message="Server Error")


def _parse_exception(cause: BaseException | None, step: str = "copy_records_to_table"):
    try:
        raise ParseException(
            message=str(cause) if cause is not None else None,
            step=step,
            resource_id="resource-1",
            url="https://example.com/data.csv",
        ) from cause
    except ParseException as e:
        return e


@pytest.mark.parametrize(
    "cause, expected",
    [
        # Allowlisted data/value errors observed in production -> republished verbatim.
        (
            ValueError("invalid literal for int() with base 10: '0.0'"),
            "copy_records_to_table:invalid literal for int() with base 10: '0.0'",
        ),
        (
            TypeError("can't subtract offset-naive and offset-aware datetimes"),
            "copy_records_to_table:can't subtract offset-naive and offset-aware datetimes",
        ),
        (
            IndexError("list index out of range"),
            "copy_records_to_table:list index out of range",
        ),
        (
            KeyError("header_row_idx"),
            "copy_records_to_table:'header_row_idx'",
        ),
        # No cause -> our own free-form message is never republished.
        (None, "copy_records_to_table:error"),
        # PostgreSQL errors -> redacted, never leak DB internals.
        (
            asyncpg.PostgresSyntaxError('syntax error at or near "1.0"'),
            "copy_records_to_table:error",
        ),
        (
            asyncpg.InvalidTextRepresentationError("invalid input syntax for type integer"),
            "copy_records_to_table:error",
        ),
        # Infrastructure errors -> redacted, never leak host/port/endpoint/bucket.
        (
            OSError("connection refused to db.internal:5432"),
            "copy_records_to_table:error",
        ),
        (
            _http_error(),
            "copy_records_to_table:error",
        ),
    ],
)
async def test_handle_parse_exception_redaction(clean_db, fake_check, cause, expected):
    check = await fake_check()
    e = _parse_exception(cause)
    updated = await handle_parse_exception(e, table_name=None, check=check)
    assert updated["parsing_error"] == expected
    assert updated["parsing_finished_at"] is not None


async def test_handle_parse_exception_csv_detective_value_error_exposed(clean_db, fake_check):
    """The most common production error (csv_detective) is a ValueError and must stay exposed."""
    check = await fake_check()
    cause = ValueError(
        "Number of columns is not even across the first 10 rows (detected separator: ;)."
    )
    e = _parse_exception(cause, step="csv_detective")
    updated = await handle_parse_exception(e, table_name=None, check=check)
    assert updated["parsing_error"] == (
        "csv_detective:Number of columns is not even across the first 10 rows "
        "(detected separator: ;)."
    )


async def test_handle_parse_exception_postgres_syntax_error_redacted(clean_db, fake_check):
    """The quote-ident-triggered PG 'syntax error at or near "1.0"' must be redacted."""
    check = await fake_check()
    e = _parse_exception(
        asyncpg.PostgresSyntaxError('syntax error at or near "1.0"'), step="parquet_export"
    )
    updated = await handle_parse_exception(e, table_name=None, check=check)
    assert updated["parsing_error"] == "parquet_export:error"


async def test_handle_parse_exception_overflow_error_redacted(clean_db, fake_check):
    """OverflowError is NOT allowlisted and must be redacted.

    It surfaces in production as the csv_detective message
    `Python int too large to convert to C long`, but it is not clearly data-only
    (a large integer cast can also originate from unexpected input), so we keep it
    redacted. This test documents that deliberate decision.
    """
    check = await fake_check()
    e = _parse_exception(
        OverflowError("Python int too large to convert to C long"), step="csv_detective"
    )
    updated = await handle_parse_exception(e, table_name=None, check=check)
    assert updated["parsing_error"] == "csv_detective:error"


async def test_handle_parse_exception_ogc_value_error_still_exposed(clean_db, fake_check):
    """Known accepted leak: owslib raises OGC connection/response errors as ValueError.

    Because ValueError is allowlisted, the message (which embeds the full endpoint URL
    and sometimes server response text) is republished verbatim. This is a KNOWN,
    accepted behaviour documented in the allowlist docstring; distinguishing it from
    safe data ValueErrors requires wrapping owslib errors in a dedicated exception
    type and is tracked separately.
    """
    check = await fake_check()
    cause = ValueError(
        "https://ogc.example/wfs/f63cfb9a?service=WFS&request=GetCapabilities responded with "
        "Content-Type 'text/html;charset=UTF-8': 'msLoadMap(): Unable to access file. "
        "(/var/www/html/data/cartes/wfs_f63cfb9a.map)'"
    )
    e = _parse_exception(cause, step="ogc_service_connection")
    updated = await handle_parse_exception(e, table_name=None, check=check)
    assert updated["parsing_error"] == (
        "ogc_service_connection:https://ogc.example/wfs/f63cfb9a?service=WFS&request=GetCapabilities "
        "responded with Content-Type 'text/html;charset=UTF-8': 'msLoadMap(): Unable to access file. "
        "(/var/www/html/data/cartes/wfs_f63cfb9a.map)'"
    )

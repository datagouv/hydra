"""Tests for Sentry error handling and automatic original exception retrieval."""

import asyncpg

from udata_hydra.utils.errors import ParseException, _is_safe_to_expose


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
    # A ParseException without an underlying cause has no cause to redact.
    assert _is_safe_to_expose(None) is True


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

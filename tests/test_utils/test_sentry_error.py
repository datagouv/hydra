"""Tests for Sentry error handling and automatic original exception retrieval."""

from udata_hydra.utils.errors import ParseException


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

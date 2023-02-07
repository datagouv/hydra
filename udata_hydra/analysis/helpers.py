from datetime import date, datetime

from dateparser import parse as date_parser
from dateutil.parser import parse as dateutil_parser, ParserError


def to_json(value: str) -> str:
    """Convenience method, should be casted from string directly by postgres"""
    return value


def _parse_dt(value: str) -> datetime:
    """For performance reasons, we try first with dateutil and fallback on dateparser"""
    try:
        return dateutil_parser(value)
    except ParserError:
        return date_parser(value)


def to_date(value: str) -> date:
    parsed = _parse_dt(value)
    return parsed.date() if parsed else None


def to_datetime(value: str) -> datetime:
    return _parse_dt(value)

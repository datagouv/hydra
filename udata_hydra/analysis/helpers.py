from datetime import date, datetime
from typing import Optional

from dateparser import parse as date_parser
from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parser


def to_json(value: str) -> str:
    """Convenience method, should be casted from string directly by postgres"""
    return value


def _parse_dt(value: str) -> Optional[datetime]:
    """For performance reasons, we try first with dateutil and fallback on dateparser"""
    try:
        return dateutil_parser(value)
    except ParserError:
        return date_parser(value)


def to_date(value: str) -> Optional[date]:
    parsed = _parse_dt(value)
    return parsed.date() if parsed else None


def to_datetime(value: str) -> Optional[datetime]:
    return _parse_dt(value)

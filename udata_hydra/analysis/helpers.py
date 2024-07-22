from datetime import date, datetime
from typing import Union

from dateparser import parse as date_parser
from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parser


def to_json(value: str) -> str:
    """Convenience method, should be casted from string directly by postgres"""
    return value


def _parse_dt(value: str) -> datetime:
    """For performance reasons, we try first with dateutil and fallback on dateparser"""
    try:
        return dateutil_parser(value)
    except ParserError:
        return date_parser(value)


def to_date(value: str) -> Union[date, None]:
    parsed = _parse_dt(value)
    return parsed.date() if parsed else None


def to_datetime(value: str) -> datetime:
    return _parse_dt(value)

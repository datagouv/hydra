from datetime import date, datetime

from dateparser import parse as date_parser


def to_json(value: str) -> str:
    """Convenience method, should be casted from string directly by postgres"""
    return value


def to_date(value: str) -> date:
    parsed = date_parser(value)
    return parsed.date() if parsed else None


def to_datetime(value: str) -> datetime:
    return date_parser(value)

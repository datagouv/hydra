from datetime import datetime, timezone
from typing import Annotated

from pydantic import PlainSerializer


def format_datetime_for_json(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat()
    utc = value.astimezone(timezone.utc)
    s = utc.isoformat()
    return s[:-6] + "Z" if s.endswith("+00:00") else s


IsoDateTime = Annotated[
    datetime,
    PlainSerializer(format_datetime_for_json, return_type=str, when_used="json"),
]

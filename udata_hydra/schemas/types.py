from datetime import datetime, timezone
from typing import Annotated

from pydantic import PlainSerializer


def format_datetime_for_json(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat()
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


IsoDateTime = Annotated[
    datetime,
    PlainSerializer(format_datetime_for_json, return_type=str, when_used="json"),
]

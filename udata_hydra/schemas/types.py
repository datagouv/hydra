from datetime import datetime
from typing import Annotated

from pydantic import PlainSerializer

# JSON mode serializes UTC datetimes as "...Z"; isoformat() keeps "+00:00" like the analysis payloads.
IsoDateTime = Annotated[
    datetime,
    PlainSerializer(lambda value: value.isoformat(), return_type=str, when_used="json"),
]

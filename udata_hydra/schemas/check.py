import datetime
import json
from typing import Optional

from pydantic import UUID4, BaseModel, Field, field_validator


class CheckSchema(BaseModel):
    check_id: int = Field(alias="id")
    catalog_id: Optional[int] = None
    url: str | None = None
    domain: str | None = None
    created_at: Optional[datetime.datetime]
    check_status: int | None = Field(alias="status")
    headers: dict | None = {}
    timeout: bool | None = None
    response_time: Optional[float]
    error: str | None = None
    dataset_id: str | None = None
    resource_id: UUID4 | None = None
    deleted: bool | None = None
    parsing_started_at: Optional[datetime.datetime] = None
    parsing_finished_at: Optional[datetime.datetime] = None
    parsing_error: str | None = None
    parsing_table: str | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def transform(cls, headers: str | None) -> dict:
        if headers:
            return json.loads(headers)
        return {}


class CheckGroupBy(BaseModel):
    value: str
    count: int

import datetime
import json
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CheckSchema(BaseModel):
    check_id: int = Field(alias="id")
    catalog_id: int | None = None
    url: str | None = None
    domain: str | None = None
    created_at: datetime.datetime | None
    check_status: int = Field(alias="status")
    headers: dict
    timeout: bool | None = None
    response_time: float | None
    error: str | None = None
    dataset_id: str | None = None
    resource_id: UUID | None = None
    deleted: bool | None = None
    parsing_started_at: datetime.datetime | None = None
    parsing_finished_at: datetime.datetime | None = None
    parsing_error: str | None = None
    parsing_table: str | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def transform(cls, obj: dict) -> dict:
        return json.loads(obj["headers"]) if obj["headers"] else {}


class CheckGroupBy(BaseModel):
    value: str
    count: int

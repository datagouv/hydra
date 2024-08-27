import datetime
import json
from typing import Optional

from pydantic import UUID1, BaseModel, Field, field_validator


class CheckSchema(BaseModel):
    check_id: int = Field(alias="id")
    catalog_id: Optional[int] = None
    url: Optional[str] = None
    domain: Optional[str] = None
    created_at: Optional[datetime.datetime]
    check_status: int = Field(alias="status")
    headers: dict
    timeout: Optional[bool] = None
    response_time: Optional[float]
    error: Optional[str] = None
    dataset_id: Optional[str] = None
    resource_id: Optional[UUID1] = None
    deleted: Optional[bool] = None
    parsing_started_at: Optional[datetime.datetime] = None
    parsing_finished_at: Optional[datetime.datetime] = None
    parsing_error: Optional[str] = None
    parsing_table: Optional[str] = None

    @field_validator("headers", mode="before")
    @classmethod
    def transform(cls, obj: dict) -> dict:
        return json.loads(obj["headers"]) if obj["headers"] else {}

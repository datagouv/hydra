import datetime
import json

from pydantic import UUID1, BaseModel, Field, field_validator


class CheckSchema(BaseModel):
    check_id: int = Field(alias="id")
    catalog_id: int | None
    url: str | None
    domain: str | None
    created_at: datetime.datetime | None
    check_status: int = Field(alias="status")
    headers: dict
    timeout: bool | None
    response_time: float | None
    error: str | None
    dataset_id: str | None
    resource_id: UUID1 | None
    deleted: bool | None
    parsing_started_at: datetime.datetime | None
    parsing_finished_at: datetime.datetime | None
    parsing_error: str | None
    parsing_table: str | None

    @field_validator("headers", mode="before")
    @classmethod
    def transform(cls, obj: dict) -> dict:
        return json.loads(obj["headers"]) if obj["headers"] else {}


class ResourceDocument(BaseModel):
    id: str
    url: str
    format: str | None
    title: str
    schema: str | None
    description: str | None
    filetype: str
    type: str
    mime: str | None
    filesize: int | None
    checksum_type: str | None
    checksum_value: str | None
    created_at: datetime.datetime
    last_modified: datetime.datetime
    extras: dict | None
    harvest: dict | None


class ResourceQuery(BaseModel):
    dataset_id: str
    resource_id: str
    document: ResourceDocument | None

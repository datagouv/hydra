import datetime
import json
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CheckSchema(BaseModel):
    # Allow creation from ORM objects (like asyncpg Record) by enabling from_attributes
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: int = Field(alias="check_id")
    catalog_id: int | None = None
    url: str | None = None
    domain: str | None = None
    created_at: datetime.datetime | None
    status: int | None = Field(alias="check_status")
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
    next_check_at: datetime.datetime | None = None
    parquet_url: str | None = None
    parquet_size: int | None = None
    pmtiles_url: str | None = None
    pmtiles_size: int | None = None
    geojson_url: str | None = None
    geojson_size: int | None = None
    detected_last_modified_at: datetime.datetime | None = None
    analysis_error: str | None = None
    checksum: str | None = None
    filesize: int | None = None
    mime_type: str | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def transform(cls, value: str | dict) -> dict:
        if isinstance(value, dict):
            return value
        return json.loads(value) if value else {}


class CheckGroupBy(BaseModel):
    # Allow creation from ORM objects (like asyncpg Record) by enabling from_attributes
    model_config = ConfigDict(from_attributes=True)

    value: str
    count: int

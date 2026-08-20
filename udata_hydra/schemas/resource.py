import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ResourceDocumentSchema(BaseModel):
    id: str
    url: str
    format: str | None = None
    title: str
    # Named schema_ because "schema" shadows an attribute of pydantic's BaseModel
    schema_: dict | None = Field(default=None, alias="schema")
    description: str | None = None
    filetype: str
    type: str
    mime: str | None = None
    filesize: int | None = None
    checksum_type: str | None = None
    checksum_value: str | None = None
    created_at: datetime.datetime
    last_modified: datetime.datetime
    extras: dict | None = None
    harvest: dict | None = None


class ResourceSchema(BaseModel):
    dataset_id: str
    resource_id: UUID
    status: str | None = None
    status_since: datetime.datetime | None = None
    document: ResourceDocumentSchema | None = None

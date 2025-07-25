import datetime
from uuid import UUID

from pydantic import BaseModel


class ResourceDocumentSchema(BaseModel):
    id: str
    url: str
    format: str | None = None
    title: str
    schema: str | None = None
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
    document: ResourceDocumentSchema | None = None

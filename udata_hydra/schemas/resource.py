import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ResourceDocumentSchema(BaseModel):
    id: str
    url: str
    format: Optional[str] = None
    title: str
    schema: Optional[str] = None
    description: Optional[str] = None
    filetype: str
    type: str
    mime: Optional[str] = None
    filesize: Optional[int]
    checksum_type: Optional[str] = None
    checksum_value: Optional[str] = None
    created_at: datetime.datetime
    last_modified: datetime.datetime
    extras: Optional[dict] = None
    harvest: Optional[dict] = None


class ResourceSchema(BaseModel):
    dataset_id: str
    resource_id: UUID
    document: Optional[ResourceDocumentSchema] = None

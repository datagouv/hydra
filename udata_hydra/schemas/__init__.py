from uuid import UUID

# Request/Response models for API endpoints
from pydantic import BaseModel

from .check import CheckGroupBy, CheckSchema
from .resource import CreateResourceRequest, ResourceDocumentSchema, ResourceSchema
from .resource_exception import ResourceExceptionSchema


class CreateResourceExceptionRequest(BaseModel):
    resource_id: UUID
    table_indexes: dict[str, str] | None = None
    comment: str | None = None


class UpdateResourceExceptionRequest(BaseModel):
    table_indexes: dict[str, str] | None = None
    comment: str | None = None


class CreateCheckRequest(BaseModel):
    resource_id: UUID
    force_analysis: bool = True


__all__ = [
    "CheckGroupBy",
    "CheckSchema",
    "CreateResourceRequest",
    "ResourceDocumentSchema",
    "ResourceSchema",
    "ResourceExceptionSchema",
    "CreateResourceExceptionRequest",
    "UpdateResourceExceptionRequest",
    "CreateCheckRequest",
]

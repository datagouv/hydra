from uuid import UUID

# Request/Response models for API endpoints
from pydantic import BaseModel, ConfigDict

from .check import CheckGroupBy, CheckSchema
from .resource import CreateResourceRequest, ResourceDocumentSchema, ResourceSchema
from .resource_exception import ResourceExceptionSchema
from .resource_index import ReplaceResourceIndexesRequest, ResourceIndexesSchema


# `table_indexes` moved to /api/resources-indexes: a client still sending it gets a 400
# instead of having its indexes silently dropped.
class CreateResourceExceptionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: UUID
    comment: str | None = None


class UpdateResourceExceptionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    "ResourceIndexesSchema",
    "ReplaceResourceIndexesRequest",
    "CreateResourceExceptionRequest",
    "UpdateResourceExceptionRequest",
    "CreateCheckRequest",
]

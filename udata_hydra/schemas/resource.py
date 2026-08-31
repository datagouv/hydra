from uuid import UUID

from pydantic import BaseModel, Field

from udata_hydra.schemas.types import IsoDateTime


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
    created_at: IsoDateTime
    last_modified: IsoDateTime
    extras: dict | None = None
    harvest: dict | None = None


class ResourceSchema(BaseModel):
    """Resource as stored in catalog, returned by GET /api/resources/{id}."""

    dataset_id: str
    resource_id: UUID
    status: str | None = None
    status_since: IsoDateTime | None = None


class CreateResourceRequest(BaseModel):
    """Webhook payload from udata to create or update a resource."""

    dataset_id: str
    resource_id: UUID
    document: ResourceDocumentSchema

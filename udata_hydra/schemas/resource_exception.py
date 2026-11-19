from uuid import UUID

from pydantic import BaseModel


class ResourceExceptionSchema(BaseModel):
    id: int  # In DB, this is SERIAL PRIMARY KEY (which is an integer), but it was a str in the schema before
    resource_id: UUID
    comment: str | None = None

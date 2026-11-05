from uuid import UUID

from pydantic import BaseModel, field_validator

from udata_hydra import config


class ResourceIndexesSchema(BaseModel):
    resource_id: UUID
    indexes: dict[str, str]


class ReplaceResourceIndexesRequest(BaseModel):
    indexes: dict[str, str]

    @field_validator("indexes")
    @classmethod
    def index_types_supported(cls, indexes: dict[str, str]) -> dict[str, str]:
        supported_types = config.SQL_INDEXES_TYPES_SUPPORTED or []
        for index_type in indexes.values():
            if index_type not in supported_types:
                raise ValueError("index type must be one of: " + ", ".join(supported_types))
        return indexes

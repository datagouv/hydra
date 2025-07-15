from uuid import UUID

from pydantic import BaseModel, ConfigDict

from udata_hydra import config


class ResourceExceptionSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    resource_id: UUID
    table_indexes: str | None = None
    comment: str | None = None

    @staticmethod
    def are_table_indexes_valid(table_indexes: dict[str, str]) -> tuple[bool, str | None]:
        """
        Check if the table_indexes are valid
        returns a tuple (valid, error), with:
            - valid: a boolean indicating if the table_indexes are valid
            - error: a string describing the error, if any
        """
        if not isinstance(table_indexes, dict):
            return (False, "table_indexes must be a dictionary")
        if table_indexes:
            supported_types = config.SQL_INDEXES_TYPES_SUPPORTED or []
            for index_type in table_indexes.values():
                if index_type not in supported_types:
                    error: str = "error, index type must be one of: " + ", ".join(supported_types)
                    return (False, error)
        return (True, None)

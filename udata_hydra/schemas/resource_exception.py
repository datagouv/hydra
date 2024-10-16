from marshmallow import Schema, fields

from udata_hydra import config


class ResourceExceptionSchema(Schema):
    id = fields.Str(required=True)
    resource_id = fields.Str(required=True)
    table_indexes = fields.Str(allow_none=True)
    comment = fields.Str(allow_none=True)

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
            for index_type in table_indexes.values():
                if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                    error: str = "error, index type must be one of: " + ", ".join(
                        config.SQL_INDEXES_TYPES_SUPPORTED
                    )
                    return (False, error)
        return (True, None)

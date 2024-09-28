from marshmallow import Schema, fields


class ResourceExceptionSchema(Schema):
    id = fields.Str(required=True)
    resource_id = fields.Str(required=True)
    table_indexes = fields.Str(allow_none=True)

from marshmallow import Schema, fields


class ResourceDocumentSchema(Schema):
    id = fields.Str(required=True)
    url = fields.Str(required=True)
    format = fields.Str(allow_none=True)
    title = fields.Str(required=True)
    schema = fields.Dict(allow_none=True)
    description = fields.Str(allow_none=True)
    filetype = fields.Str(required=True)
    type = fields.Str(required=True)
    mime = fields.Str(allow_none=True)
    filesize = fields.Int(allow_none=True)
    checksum_type = fields.Str(allow_none=True)
    checksum_value = fields.Str(allow_none=True)
    created_at = fields.DateTime(required=True)
    last_modified = fields.DateTime(required=True)
    extras = fields.Dict()
    harvest = fields.Dict()


class ResourceSchema(Schema):
    dataset_id = fields.Str(required=True)
    resource_id = fields.Str(required=True)
    status = fields.Str(required=False)
    status_since = fields.DateTime(required=False)
    document = fields.Nested(ResourceDocumentSchema(), allow_none=True)

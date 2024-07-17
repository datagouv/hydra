import json

from marshmallow import Schema, fields


class CheckSchema(Schema):
    check_id = fields.Integer(data_key="id")
    catalog_id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    check_status = fields.Integer(data_key="status")
    headers = fields.Function(lambda obj: json.loads(obj["headers"]) if obj["headers"] else {})
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    deleted = fields.Boolean()
    parsing_started_at = fields.DateTime()
    parsing_finished_at = fields.DateTime()
    parsing_error = fields.Str()
    parsing_table = fields.Str()


class ResourceDocument(Schema):
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


class ResourceQuery(Schema):
    dataset_id = fields.Str(required=True)
    resource_id = fields.Str(required=True)
    document = fields.Nested(ResourceDocument(), allow_none=True)

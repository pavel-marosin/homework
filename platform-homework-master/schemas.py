from marshmallow import Schema, fields
from marshmallow.validate import Range, OneOf


class SensorInputClass(Schema):
    type = fields.Str(required=True, validate=OneOf(['temperature', 'humidity']))
    value = fields.Integer(required=True, validate=Range(min=0, max=100))
    date_created = fields.Integer(required=False)
    device_uuid = fields.String()

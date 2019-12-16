from functools import wraps
from constants import ALLOWED_SENSORS
from flask import request, jsonify


def validate_type(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        device_type = request.args.get('type')
        if not device_type or device_type not in ALLOWED_SENSORS:
            return jsonify({"message": "a valid type parameter is required"}), 400

        return f(*args, **kwargs)

    return wrapper

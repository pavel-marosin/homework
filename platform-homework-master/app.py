from flask import Flask, request,  abort
from flask.json import jsonify
import time
import json

from decorators import validate_type
from db import get_db
from stats import analyze_data
from schemas import SensorInputClass

app = Flask(__name__)

sensor_input_schema = SensorInputClass()

@app.route('/devices/<string:device_uuid>/readings/', methods = ['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    conn = get_db()
    cur = conn.cursor()

    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)
        #validate request data and return any errors if applicable
        errors = sensor_input_schema.validate(post_data)
        if errors:
            abort(400, str(errors))

        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))

        conn.commit()

        # Return success
        return 'success', 201
    else:
        # Execute the query
        filters = 'device_uuid = "{}"'.format(device_uuid)
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        device_type = request.args.get('type')

        if device_type:
            filters = filters + ' and type = "{}"'.format(device_type)
        if start_date:
            filters = filters + ' and date_created >= {}'.format(start_date)
        if end_date:
            filters = filters + ' and date_created <= {}'.format(end_date)

        sql = 'select * from readings where {}'.format(filters)
        cur.execute(sql)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/min/', methods=['GET'])
@validate_type
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if not device_type:
        return jsonify({"message": "type is a required parameter"}), 400

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(filters)

    try:
        min_value = analyze_data(sql, "min")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code

    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "value": int(min_value)
    }

    return jsonify(response_dict), 200


@app.route('/devices/<string:device_uuid>/readings/max/', methods=['GET'])
@validate_type
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(
        filters)
    try:
        max_value = analyze_data(sql, "max")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code

    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "value": int(max_value)
    }

    return jsonify(response_dict), 200


@app.route('/devices/<string:device_uuid>/readings/median/', methods = ['GET'])
@validate_type
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(
        filters)
    try:
        median = analyze_data(sql, "median")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code

    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "value": median
    }

    return jsonify(response_dict), 200


@app.route('/devices/<string:device_uuid>/readings/mean/', methods = ['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(
        filters)
    try:
        mean = analyze_data(sql, "mean")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code

    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "value": mean
    }

    return jsonify(response_dict), 200


@app.route('/devices/<string:device_uuid>/readings/mode/', methods = ['GET'])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(
        filters)
    try:
        mode = analyze_data(sql, "mode")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code
    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "value": int(mode[0]) if len(mode) == 1 else None
    }

    return jsonify(response_dict), 200

@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods = ['GET'])
@validate_type
def request_device_readings_mode_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    start_date = request.args.get('start')
    end_date = request.args.get('end')
    device_type = request.args.get('type')

    filters = 'device_uuid = "{}"'.format(device_uuid)

    if device_type:
        filters = filters + ' and type = "{}"'.format(device_type)
    if start_date:
        filters = filters + ' and date_created >= {}'.format(start_date)
    if end_date:
        filters = filters + ' and date_created <= {}'.format(end_date)

    sql = 'select device_uuid, type, value, date_created from readings where {}'.format(
        filters)
    try:
        quartiles = analyze_data(sql, "quarterlies")
    except Exception as e:
        return jsonify({"message": "Error when processing request"}), e.code

    response_dict = {
        "device_uuid": device_uuid,
        "device_type": device_type,
        "first_quartile": int(quartiles.iloc[0]),
        "third_quartile": int(quartiles.iloc[1])
    }

    return jsonify(response_dict), 200


if __name__ == '__main__':
    app.run()

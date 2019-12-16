import json
import pytest
import sqlite3
import time
import unittest

from app import app
from db import get_db


class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        self.device_uuid = 'test_device'
        #
        # # Setup some sensor data
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 40))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))
        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        # self.assertTrue(len(json.loads(request.data)) == 3)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db

        conn = get_db()
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        # rows = cur.fetchall()
        #
        # # We should have four
        # # self.assertTrue(len(rows) == 4)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        request = self.client().get('/devices/{0}/readings/?type=temperature'.format(self.device_uuid))
        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        request = self.client().get('/devices/{0}/readings/?type=humidity'.format(self.device_uuid))
        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        request = self.client().get(
            '/devices/{0}/readings/?type=temperature&end={1}'.format(self.device_uuid, int(time.time()) - 45))

        self.assertTrue(len(json.loads(request.data)) == 2)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        request = self.client().get(
            '/devices/{0}/readings/min/?type=temperature'.format(self.device_uuid))
        response_data = json.loads(request.data)
        min_val = response_data.get("value")

        self.assertTrue(min_val == 22)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get(
            '/devices/{0}/readings/max/?type=temperature'.format(self.device_uuid))
        response_data = json.loads(request.data)
        max_val = response_data.get("value")

        self.assertTrue(max_val == 100)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get(
            '/devices/{0}/readings/median/?type=temperature'.format(self.device_uuid))
        response_data = json.loads(request.data)
        median_val = response_data.get("value")

        self.assertTrue(median_val == 36)

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get(
            '/devices/{0}/readings/mean/?type=temperature'.format(self.device_uuid))
        response_data = json.loads(request.data)
        mean_val = response_data.get("value")

        self.assertTrue(mean_val == 48.5)

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        request = self.client().get(
            '/devices/{0}/readings/mode/?type=temperature'.format(self.device_uuid))
        response_data = json.loads(request.data)
        mode_val = response_data.get("value")

        self.assertTrue(mode_val == 22)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get(
            '/devices/{0}/readings/quartiles/?type=temperature'.format(self.device_uuid))

        response_data = json.loads(request.data)
        # 1st quartile - 22
        # 3rd quartile - 100

        self.assertTrue(response_data.get("first_quartile") == 22)
        self.assertTrue(response_data.get("third_quartile") == 100)


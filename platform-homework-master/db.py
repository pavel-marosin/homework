from flask import Flask
import sqlite3

app = Flask(__name__)


def get_db():
    conn = sqlite3.connect('database.db')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
    conn.close()

    if app.config['TESTING']:
        # sql_delete_query = """DELETE from SqliteDb_developers where id = 6"""
        connection = sqlite3.connect('test_database.db')
        connection.execute("DELETE from readings where id !=0")

    else:
        connection = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row

    return connection

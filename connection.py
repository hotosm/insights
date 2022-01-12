
"""Responsible for database connection with environment variable can be executed first and get connection parameter back and use it in your python file

Returns:
    [connection_dictionary]: [Database Connection param dict : database,user,pass,host,port respectively]
"""

import config
import sys
import psycopg2

def get_connection_param():
    database_connection_parameters = dict(
        dbname = config.DATABASE_NAME,
        user = config.DATABASE_USER,
        password = config.DATABASE_PASSWORD,
        host =   config.DATABASE_HOST,
        port = config.DATABASE_PORT,
    )
    try:
        conn = psycopg2.connect(
           **database_connection_parameters
        )
        print("Database Connection parameters are valid")
        print(get_connection_param)
        return database_connection_parameters
    except psycopg2.OperationalError as err:
        print("Connection error: Please recheck the connection parameters")
        print("Current connection parameters:")
        database_connection_parameters['password'] = f"{type(database_connection_parameters['password'])}(**VALUE REDACTED**)"
        print(database_connection_parameters)
        sys.exit(1)

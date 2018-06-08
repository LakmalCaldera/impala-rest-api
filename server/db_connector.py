from impala.dbapi import connect
from flask import current_app


def create_db_connection():
    """
        Get a simple connection to HiveServer2 (HS2).
    """

    db_host = current_app.config['IMPALA_HOST']
    db_port = current_app.config['IMPALA_PORT']
    db_name = current_app.config['IMPALA_DATABASE']

    print("Connecting to Impala on {0}:{1}:{2}".format(db_host, db_port, db_name))
    conn = connect(host=db_host, port=db_port, database=db_name)
    return conn

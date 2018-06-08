from flask import Flask, request, jsonify
from impala.error import Error, DatabaseError, OperationalError
from server.db_connector import create_db_connection
from server.query_db import run_query
from server.query_builder import get_sql_for_sw_count


def init_config(application):
    application.config.from_object('server.config')


def create_app():
    application = Flask(__name__)
    init_config(application)
    return application


app = create_app()


@app.route("/softwares")
def getCount(query_table='software_latest'):

    # List of Tuple params
    params = request.args

    if len(params) > 0:
        # Create db connection
        conn = None
        try:
            conn = create_db_connection()
        except OperationalError as error:
            return jsonify({'error': error})

        try:
            with conn.cursor() as cursor:
                # Results
                count = 0

                # Build SQL
                sql = get_sql_for_sw_count(len(params), query_table)

                # Run SQL query and get count. Don't query database only if params is passed.
                count = run_query(sql, cursor, params)[0][0]

                # If no results set from db, set count to zero
                if count is None:
                    count = 0

                # Return JSON object {count: result}
                return jsonify({'count': count})
        except Exception as e:
            # Handle Error
            op = cursor._last_operation
            return jsonify({'error': op.get_log()})
        finally:
            # Release connection
            conn.close()
    else:
        # Invalid Usage of service
        return jsonify({'error': 'You must provide a valid query string. Example - /softwares?swproduct=...&customer=...&...'})


@app.errorhandler(404)
def page_not_found(e):
    return jsonify({'error': 'Resource not found'}), 400


@app.errorhandler(DatabaseError)
def handle_invalid_usage(error):
    return jsonify({'error': 'Database Error'}), 500

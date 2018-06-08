from impala.dbapi import connect


def run_query(sql, cursor, params=None):
    """
        Execute Query passed into the method

        @sql    - <str> SQL Query to execute.
        @conn   - Connection to HiveServer2 (HS2).
    """
    tupleParams = tuple(
        [item for param in params.items() for item in param])
    cursor.execute(sql.format(*tupleParams))
    # cursor.execute(sql, (*tupleParams))
    results = cursor.fetchall()
    return results

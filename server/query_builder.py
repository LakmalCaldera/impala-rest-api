
def get_sql_for_sw_count(paramCount, table_name):
    """
        Parse params and return dynamic sql that can be used to extract the record count

        @params        - <dic> contain table column names and filter value
        @table_name    - <str> database table name that you want to query
        @return        - <str> SQL Query String
    """

    sql = 'SELECT count(*) from {0} WHERE'.format(table_name)
    sql += ' AND '.join([' {}="{}" ' for index in range(paramCount)])
    return sql

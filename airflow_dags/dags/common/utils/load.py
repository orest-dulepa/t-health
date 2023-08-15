def format_sql_row_item(row_item):
    if 'nan' in str(row_item) or row_item == '' or row_item == ' ':
        return 'null'

    row_item = str(row_item).replace("'", "''")

    return f"'{row_item}'"

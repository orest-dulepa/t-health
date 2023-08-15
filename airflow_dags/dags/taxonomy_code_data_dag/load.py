from typing import List

from common.utils.load import format_sql_row_item

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_data_to_db(**context) -> None:
    data: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_data__task')

    if len(data) == 0:
        return

    conn_name = Variable.get('taxonomy_code_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql_values = ''

    for data_item in data:
        for main_row_data in data_item['data']:
            main_row_data = [format_sql_row_item(main_row_data_item) for main_row_data_item in main_row_data]

            sql_values += '('
            sql_values += ', '.join(main_row_data)
            sql_values += f", '{data_item['date']}'"
            sql_values += '), '

    sql_values = sql_values[:-2]
    sql_values += ';'

    sql = f"""
        INSERT INTO internal_reference.ref_nucc_taxonomy_code (
            code, "grouping", classification, specialization, definition, effective_date, deactivation_date,
            last_modified_date, notes, display_name, section, "date"
        ) VALUES {sql_values}
    """

    hook.run(sql)

from typing import List

from common.utils.load import format_sql_row_item

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_data_to_db(**context) -> None:
    data: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_data__task')

    if len(data) == 0:
        return

    conn_name = Variable.get('cms_gpci_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql_values = ''

    for main_row_data in data:
        main_row_data = [format_sql_row_item(main_row_data_item) for main_row_data_item in main_row_data]

        sql_values += '('
        sql_values += ', '.join(main_row_data)
        sql_values += '), '

    sql_values = sql_values[:-2]
    sql_values += ';'

    sql = f"""
        INSERT INTO internal_reference.ref_cms_gpci (
            carrier, state, locality_number, locality_name, pw_gpci, pe_gpci, mp_gpci, eff_start_dt, eff_end_dt
        ) VALUES {sql_values}
    """

    hook.run(sql)

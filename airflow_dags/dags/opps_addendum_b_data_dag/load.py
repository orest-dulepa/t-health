from typing import List

from common.utils.load import format_sql_row_item

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_data_to_db(**context) -> None:
    data: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_data__task')

    if len(data) == 0:
        return

    conn_name = Variable.get('opps_addendum_b_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql_values = ''

    for data_item in data:
        for main_row_data in data_item['data']:
            main_row_data = [format_sql_row_item(main_row_data_item) for main_row_data_item in main_row_data]

            sql_values += '('
            sql_values += ', '.join(main_row_data)
            sql_values += f", '{data_item['date_of_first_day_of_quarter']}'"
            sql_values += f", '{data_item['date_of_last_day_of_quarter']}'"
            sql_values += '), '

    sql_values = sql_values[:-2]
    sql_values += ';'

    sql = f"""
        INSERT INTO internal_reference.ref_cms_opps_addendum_b (
            hcpcs, short_descriptor, si, apc, relative_weight, payment_rate, national_unadjusted_copayment,
            minimum_unadjusted_copayment, notes, drug_pass_through_expiration_during_calendar_year,
            asterix_indicates_change, eff_start_dt, eff_end_dt
        ) VALUES {sql_values}
    """

    hook.run(sql)

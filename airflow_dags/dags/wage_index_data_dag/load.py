from typing import List

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_wage_indexes_to_db(**context) -> None:
    wage_data: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_wage_indexes__task')

    conn_name = Variable.get('wage_index_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    for wage_data_item in wage_data:
        version = 1.0 if wage_data_item['version'] == 'FR' else 1.1

        for wage_indexes in wage_data_item['wage_indexes']:
            sql = """
                INSERT INTO internal_reference.ref_cms_wage_index_by_cbsa (
                    year, version, cbsa, area_name, state, state_code, hourly_wage, hourly_wage_3yr, wage_index, gaf,
                    reclass_wi, reclass_gaf, frontier_wage_elig, rural_floor_elig, pre_floor_wi,
                    reclass_frontier_wi, reclass_rural_wi, reclass_pre_floor_wi
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            hook.run(sql, parameters=(wage_data_item['year'], version, *wage_indexes))

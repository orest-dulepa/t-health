import os
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any

from common.utils import get_param

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_data(**context: Dict[str, Any]) -> List[dict]:
    folder_name = get_param('local_folder_name', **context)
    data = []

    for file_name in os.listdir(folder_name):
        _date = datetime.strptime(file_name.split('.')[0], '%y-%m-%d')

        if is_current_date_in_db(_date):
            continue

        needed_columns = [
            'Code', 'Grouping', 'Classification', 'Specialization', 'Definition', 'Effective Date', 'Deactivation Date',
            'Last Modified Date', 'Notes', 'Display Name', 'Section'
        ]

        with open(f'{folder_name}/{file_name}', 'r') as file:
            df = pd.read_csv(file, encoding='unicode_escape')

            csv_data = []

            for row_number in range(len(df)):
                csv_data.append([])

                for column in needed_columns:
                    if df.get([column]) is not None:
                        csv_data[row_number].append(df.get([column]).values[row_number][0])
                    else:
                        csv_data[row_number].append('nan')

            data.append({'date': str(_date), 'data': csv_data})

    return data


def is_current_date_in_db(_date: date) -> bool:
    conn_name = Variable.get('taxonomy_code_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(code) FROM internal_reference.ref_nucc_taxonomy_code WHERE date = %s'

    count = hook.get_first(sql, parameters=(_date,))[0]

    return count > 0

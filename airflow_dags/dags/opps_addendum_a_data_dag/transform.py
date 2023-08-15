import os
from datetime import date
from typing import List, Dict, Any

import zipfile
import pandas as pd

from common.utils import get_param
from common.utils.quarters import get_date_of_first_day_of_quarter, get_date_of_last_day_of_quarter

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_data(**context: Dict[str, Any]) -> List[dict]:
    folder_name = get_param('local_folder_name', **context)
    data = []

    for zip_name in sorted(os.listdir(folder_name)):
        archive = zipfile.ZipFile(f'{folder_name}/{zip_name}', 'r')

        year, quarter = zip_name.replace('.zip', '').split('_')[:2]
        year = int(year)
        quarter = int(quarter)
        is_correction = 'c' in zip_name

        date_of_first_day_of_quarter = get_date_of_first_day_of_quarter(year, quarter)
        date_of_last_day_of_quarter = get_date_of_last_day_of_quarter(year, quarter)

        if is_correction:
            remove_quarter_in_db(date_of_first_day_of_quarter, date_of_last_day_of_quarter)
        elif is_current_quarter_in_db(date_of_first_day_of_quarter, date_of_last_day_of_quarter):
            continue

        for file in archive.filelist:
            file_name = file.filename.lower()

            if f'addendum_a' in file_name and '.csv' in file_name:
                with archive.open(file.filename) as data_file:
                    df = pd.read_csv(data_file, encoding='unicode_escape')

                    if len(df.columns) == 13:
                        df = df.drop(columns=df.iloc[:, 9:12])

                    data.append({
                        'date_of_first_day_of_quarter': str(date_of_first_day_of_quarter),
                        'date_of_last_day_of_quarter': str(date_of_last_day_of_quarter),
                        # remove first 1 row and convert to list
                        'data': df[1:].values.tolist()
                    })

    return data


def is_current_quarter_in_db(date_of_first_day_of_quarter: date, date_of_last_day_of_quarter: date) -> bool:
    conn_name = Variable.get('opps_addendum_a_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(apc) FROM internal_reference.ref_cms_opps_addendum_a WHERE eff_start_dt = %s AND eff_end_dt = %s'

    count = hook.get_first(sql, parameters=(date_of_first_day_of_quarter, date_of_last_day_of_quarter))[0]

    return count > 0


def remove_quarter_in_db(date_of_first_day_of_quarter: date, date_of_last_day_of_quarter: date) -> None:
    conn_name = Variable.get('opps_addendum_a_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'DELETE FROM internal_reference.ref_cms_opps_addendum_a WHERE eff_start_dt = %s AND eff_end_dt = %s'

    hook.run(sql, parameters=(date_of_first_day_of_quarter, date_of_last_day_of_quarter))

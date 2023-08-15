import os
from datetime import date
from typing import List, Dict, Any

import zipfile
import pandas as pd

from common.utils import get_param
from common.utils.quarters import get_date_of_first_day_of_quarter, get_date_of_last_day_of_quarter

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_pfs_rvu(**context: Dict[str, Any]) -> List[dict]:
    folder_name = get_param('local_folder_name', **context)
    all_pfs_rvu = []

    for zip_name in os.listdir(folder_name):
        archive = zipfile.ZipFile(f'{folder_name}/{zip_name}', 'r')

        year = int(zip_name.split('_')[0])
        quarter = int(zip_name.split('_')[1].split('.')[0])
        date_of_first_day_of_quarter = get_date_of_first_day_of_quarter(year, quarter)
        date_of_last_day_of_quarter = get_date_of_last_day_of_quarter(year, quarter)

        if is_current_quarter_in_db(date_of_first_day_of_quarter, date_of_last_day_of_quarter):
            continue

        for file in archive.filelist:
            if 'PPRRVU' in file.filename and '.csv' in file.filename:
                with archive.open(file.filename) as pfs_rvu_file:
                    df = pd.read_csv(pfs_rvu_file, encoding='unicode_escape')
                    all_pfs_rvu.append({
                        'date_of_first_day_of_quarter': str(date_of_first_day_of_quarter),
                        'date_of_last_day_of_quarter': str(date_of_last_day_of_quarter),
                        # remove first 10 rows, 1 last and convert to list
                        'data': df[9:-1].values.tolist()
                    })

    return all_pfs_rvu


def is_current_quarter_in_db(date_of_first_day_of_quarter: date, date_of_last_day_of_quarter: date) -> bool:
    conn_name = Variable.get('pfs_rvu_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(hcpcs) FROM internal_reference.ref_cms_pfs_rvu WHERE eff_start_dt = %s AND eff_end_dt = %s'

    count = hook.get_first(sql, parameters=(date_of_first_day_of_quarter, date_of_last_day_of_quarter))[0]

    return count > 0

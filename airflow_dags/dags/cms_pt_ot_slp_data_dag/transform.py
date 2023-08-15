import re
import zipfile
import pandas as pd
from typing import List, Dict, Any

from common.utils import get_param

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_data(**context: Dict[str, Any]) -> List[dict]:
    folder_name = get_param('local_folder_name', **context)
    data = []

    archive = zipfile.ZipFile(f'{folder_name}/data.zip', 'r')

    for file in archive.filelist:
        if '.csv' not in file.filename:
            continue

        year = int(re.findall(r'\d{4}', file.filename)[0])

        if is_current_date_in_db(year):
            continue

        with archive.open(file.filename) as csv_file:
            df = pd.read_csv(csv_file, encoding='unicode_escape')
            # remove first 3 rows, get only first 7 columns and convert to list
            df = df[3:].iloc[:, :7]
            cms_pt_ot_slp_data = []

            for row_number in range(len(df)):
                row = df.values[row_number].tolist()

                if pd.isna(row[0]):
                    break

                cms_pt_ot_slp_data.append(row)

        data.append({'eff_start_dt': f'{year}/01/01', 'eff_end_dt': f'{year}/12/31', 'data': cms_pt_ot_slp_data})

    return data


def is_current_date_in_db(year: int) -> bool:
    conn_name = Variable.get('cms_pt_ot_slp_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(code) FROM internal_reference.ref_cms_opps_pt_ot_services WHERE eff_start_dt = %s'

    date = f'{year}/01/01'
    count = hook.get_first(sql, parameters=(date,))[0]

    return count > 0

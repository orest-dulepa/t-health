import re
import zipfile
from itertools import groupby
from operator import itemgetter
from typing import List, Dict, Any

from common.utils import get_param

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_data(**context: Dict[str, Any]) -> List[list]:
    folder_name = get_param('local_folder_name', **context)
    archive = zipfile.ZipFile(f'{folder_name}/data.zip', 'r')

    for file in archive.filelist:
        if not all([x in file.filename.lower() for x in ['addendum e', '.txt']]):
            continue

        archive.extract(file.filename, folder_name)

        with open(f'{folder_name}/{file.filename}', 'rb') as txt_file:
            rows = str(txt_file.read().decode('utf-16').encode('utf-8')).split('\\r')
            columns_names_row_index = 0
            main_data = []

            for index, row in enumerate(rows):
                if 'Carrier' in row:
                    columns_names_row_index = index
                    break

            for row in rows[columns_names_row_index + 1:]:
                columns = row.replace('\\n', '').split('\\t')[:17]
                if not columns[0].isnumeric():
                    break

                main_data.append(columns)

        gpci_data = get_gpci_data(rows[columns_names_row_index].split('\\t'))
        gpci_data = format_gpci_data(gpci_data)

        return combine_main_and_gpci_data(main_data, gpci_data)


def combine_main_and_gpci_data(main_data: List[list], gpci_data: List[dict]) -> List[list]:
    combined_data = []

    for gpci_data_item in gpci_data:
        eff_start_dt = gpci_data_item['eff_start_dt']
        eff_end_dt = gpci_data_item['eff_end_dt']

        if is_current_date_in_db(eff_start_dt, eff_end_dt):
            continue

        for main_data_item in main_data:
            combined_data.append([
                main_data_item[0],
                main_data_item[1],
                main_data_item[2],
                main_data_item[3],
                main_data_item[gpci_data_item['pw_gpci_column_index']],
                main_data_item[gpci_data_item['pe_gpci_column_index']],
                main_data_item[gpci_data_item['mp_gpci_column_index']],
                eff_start_dt,
                eff_end_dt,
            ])

    return combined_data


def format_gpci_data(gpci_data: List[dict]) -> List[dict]:
    grouped_gpci_data_by_date = [
        [*x, list(y)] for x, y in groupby(gpci_data, key=itemgetter('eff_start_dt', 'eff_end_dt'))
    ]
    gpci_data = []

    for grouped_gpci_data_by_date_item in grouped_gpci_data_by_date:
        gpci_data.append({
            'eff_start_dt': grouped_gpci_data_by_date_item[0],
            'eff_end_dt': grouped_gpci_data_by_date_item[1],
            'pw_gpci_column_index': get_gpci_column_index(grouped_gpci_data_by_date_item[2], 'pw gpci'),
            'pe_gpci_column_index': get_gpci_column_index(grouped_gpci_data_by_date_item[2], 'pe gpci'),
            'mp_gpci_column_index': get_gpci_column_index(grouped_gpci_data_by_date_item[2], 'mp gpci'),
        })

    return gpci_data


def get_gpci_column_index(all_gpci_types: List[dict], gpci_type: str) -> int:
    gpci_column_indexes = [
        x['column_index'] for x in all_gpci_types if x['name'] == gpci_type
    ]

    return -1 if len(gpci_column_indexes) == 0 else gpci_column_indexes[0]


def get_gpci_data(columns_names: list):
    needed_columns_names_with_date = ['pw gpci', 'pe gpci', 'mp gpci']
    gpci_data = []

    for index, column_name in enumerate(columns_names):
        for needed_column_name_with_date in needed_columns_names_with_date:
            if needed_column_name_with_date in column_name.lower():
                year = re.findall(r'\d{4}', column_name)[0]
                is_with_floor = 'with 1.0 floor' in column_name.lower()
                is_without_floor = 'without 1.0 floor' in column_name.lower()

                start_month_and_day = '01/01'
                end_month_and_day = '12/31'

                if is_with_floor:
                    start_month_and_day = '01/01'
                    end_month_and_day = '12/10'
                elif is_without_floor:
                    start_month_and_day = '12/11'
                    end_month_and_day = '12/31'

                eff_start_dt = f'{year}/{start_month_and_day}'
                eff_end_dt = f'{year}/{end_month_and_day}'

                gpci_data.append({
                    'name': needed_column_name_with_date,
                    'eff_start_dt': eff_start_dt,
                    'eff_end_dt': eff_end_dt,
                    'column_index': index
                })

    return gpci_data


def is_current_date_in_db(eff_start_dt: str, eff_end_dt: str) -> bool:
    conn_name = Variable.get('cms_gpci_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(carrier) FROM internal_reference.ref_cms_gpci WHERE eff_start_dt = %s AND eff_end_dt = %s'

    count = hook.get_first(sql, parameters=(eff_start_dt, eff_end_dt))[0]

    return count > 0

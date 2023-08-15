import os
import re
import zipfile
from zipfile import ZipInfo
from typing import List, Dict, Any

from common.utils import get_param
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_wage_indexes(**context: Dict[str, Any]) -> List[dict]:
    folder_name = get_param('local_folder_name', **context)
    all_wage_indexes = []

    for zip_name in os.listdir(folder_name):
        archive = zipfile.ZipFile(f'{folder_name}/{zip_name}', 'r')

        cn_and_fr_wage_index_file_names = get_cn_and_fr_wage_index_file_names(archive.filelist)
        year = int(zip_name.replace('.zip', ''))

        if not is_current_year_and_version_in_db(year, 'CN'):
            get_cn_or_fr_wage_indexes(all_wage_indexes, cn_and_fr_wage_index_file_names, 'CN', year, archive)
        if not is_current_year_and_version_in_db(year, 'FR'):
            get_cn_or_fr_wage_indexes(all_wage_indexes, cn_and_fr_wage_index_file_names, 'FR', year, archive)

    return [x for x in all_wage_indexes if x is not None]


def get_cn_or_fr_wage_indexes(
        all_wage_indexes: list, cn_and_fr_wage_index_file_names: Dict[str, str],
        version: str, year: int, archive) -> None:
    wage_index_file_name = cn_and_fr_wage_index_file_names.get(version)

    if not wage_index_file_name:
        return None

    wage_index_file = archive.open(wage_index_file_name)
    wage_indexes = get_wage_indexes_from_file(wage_index_file)

    all_wage_indexes.append({'year': year, 'version': version, 'wage_indexes': wage_indexes})


# cn - Correction Notice; fr - Final Rule
def get_cn_and_fr_wage_index_file_names(file_list: List[ZipInfo]) -> Dict[str, str]:
    possible_file_names = [
        {'version': 'CN', 'name': 'cn table 3.txt'},
        {'version': 'CN', 'name': 'cn_table_3.txt'},
        {'version': 'FR', 'name': 'f table 3.txt'},
        {'version': 'FR', 'name': 'f_table_3.txt'},
        {'version': 'FR', 'name': 'fr table 3.txt'},
        {'version': 'FR', 'name': 'fr_table_3.txt'},
    ]

    cn_and_fr_wage_index_file_names = {}

    for file in file_list:
        for end_name in possible_file_names:
            if end_name['name'] in file.filename.lower():
                cn_and_fr_wage_index_file_names[end_name['version']] = file.filename

    return cn_and_fr_wage_index_file_names


def get_wage_indexes_from_file(wage_index_file) -> List[list]:
    wage_index_file_content = str(wage_index_file.read())
    column_names = 'CBSA\\t' + wage_index_file_content \
        .split('\\nCBSA\\t')[1] \
        .split('Rural Floor\\r')[0] + 'Rural Floor'

    needed_column_names = [
        'CBSA',
        'Area Name',
        'State',
        'State Code',
        '2FY  Average Hourly Wage',
        '23-Year Average Hourly Wage (, , )',
        'Wage Index',
        'GAF',
        'Reclassified Wage Index',
        'Reclassified GAF',
        'Eligible for Frontier Wage Index',
        '4Eligible for Rural Floor Wage Index',
        '3Pre-Frontier and/or Pre-Rural Floor Wage Index',
        'Reclassified Wage Index Eligible for Frontier Wage Index',
        '4Reclassified Wage Index Eligible for Rural Floor Wage Index',
        '3Reclassified Wage Index Pre-Frontier and/or Pre-Rural Floor',
    ]

    file_columns_ordering = []
    for index, column_name in enumerate(column_names.split('\\t')):
        # Made column names the same as expected
        column_name = re.sub(r'\d{4}', '', column_name)
        column_name = column_name \
            .replace('\\n', ' ') \
            .replace('"', '') \
            .replace('and/or Pre-Imputed Floor ', '')

        if column_name in needed_column_names:
            file_columns_ordering.append(index)

    wage_indexes = []
    for line in wage_index_file_content.split('\\n'):
        columns = str(line).replace('\\r', '').split('\\t')
        columns = list(map(lambda _column: None if _column in ['', ' '] else _column, columns))

        if not str(columns[0]).isnumeric():
            continue

        filtered_columns = []
        for index, column in enumerate(columns):
            if index in file_columns_ordering:
                filtered_columns.append(column)

        wage_indexes.append(filtered_columns)

    return wage_indexes


def is_current_year_and_version_in_db(year: int, version: str) -> bool:
    version = 1.0 if version == 'FR' else 1.1
    conn_name = Variable.get('wage_index_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql = 'SELECT COUNT(cbsa) FROM internal_reference.ref_cms_wage_index_by_cbsa WHERE year = %s AND version = %s'

    count = hook.get_first(sql, parameters=(year, version))[0]

    return count > 0

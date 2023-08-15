import os
import requests
from typing import Union, List
from bs4 import BeautifulSoup
from common.utils import get_param


def get_all_download_urls(**context) -> List[dict]:
    root_url = get_param('root_url', **context)
    addendum_type = get_param('addendum_type', **context)

    res = requests.get(root_url)
    soup = BeautifulSoup(res.content, 'html.parser')

    links = []

    for table_row in soup.find_all('tr'):
        second_column = table_row.select_one('td.views-field-dlf-2-subject')

        if second_column is None:
            continue

        if f'addendum {addendum_type}' in str(second_column.string).lower():
            link = table_row.select_one('td.views-field-dlf-1-release-date a')

            quarters_start_months = ['January', 'April', 'July', 'October']

            quarter_start_month, year = str(link.string).split(' ')[:2]
            quarter = quarters_start_months.index(quarter_start_month) + 1
            is_correction = 'correction' in str(link.string).lower()

            links.append({
                'url': get_zip_file_url(link.get('href')),
                'year': year,
                'quarter': quarter,
                'is_correction': is_correction
            })

    return links


def get_zip_file_url(url: str) -> Union[str, None]:
    res = requests.get(f'https://www.cms.gov{url}')
    soup = BeautifulSoup(res.content, 'html.parser')
    link = soup.select_one('li.field__item a').get('href')
    link = str(link).replace('/apps/ama/license.asp?file=', '')

    return link


def download_files(**context) -> None:
    links: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_download_urls__task')
    folder_name = get_param('local_folder_name', **context)

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    for link in links:
        res = requests.get(link['url'])
        archive_name = f"{folder_name}/{link['year']}_{link['quarter']}{'_c' if link['is_correction'] else ''}.zip"

        with open(archive_name, 'wb') as file:
            file.write(res.content)

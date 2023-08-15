import os
import requests
from typing import Dict, Union

from common.utils import get_param
from bs4 import BeautifulSoup


def get_all_download_urls() -> Dict[int, str]:
    res = requests.get('https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/Wage-Index-Files')
    html_doc = res.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    links = {}

    for link in soup.find_all('a', href=True):
        link_text = str(link.string)

        # If link text is like "FY <year> Wage Index Home Page"
        if link_text.startswith('FY') and link_text.endswith('Wage Index Home Page'):
            year = int(link_text.split(' ')[1])
            wage_index_home_url = f"https://www.cms.gov{link['href']}"

            zip_file_url = get_zip_file_url(wage_index_home_url)

            if zip_file_url:
                links[year] = f'https://www.cms.gov{zip_file_url}'

    return links


def get_zip_file_url(wage_index_home_url: str) -> Union[str, None]:
    res = requests.get(wage_index_home_url)
    html_doc = res.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    zip_file_url = None

    for link in soup.find_all('a', href=True):
        link_title = str(link.get('title'))

        if 'Wage Index Tables' in link_title:
            zip_file_url = link.get('href')
            if 'Final Rule and Correction Notice' in link_title:
                zip_file_url = link.get('href')

    return zip_file_url


def download_wage_index_files(**context) -> None:
    links: Dict[int, str] = context['ti'].xcom_pull(task_ids='main__get_all_download_urls__task')
    folder_name = get_param('local_folder_name', **context)

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    for link in links.items():
        year, url = link
        res = requests.get(url)

        with open(f'{folder_name}/{year}.zip', 'wb') as file:
            file.write(res.content)

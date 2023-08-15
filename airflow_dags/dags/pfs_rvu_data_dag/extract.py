import os
import re
import requests
from typing import List, Union

from bs4 import BeautifulSoup
from common.utils import get_param


def get_all_download_urls(**context) -> List[dict]:
    root_url = get_param('root_url', **context)
    res = requests.get(root_url)
    html_doc = res.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    links = []

    for link in soup.find_all('a', href=True):
        if re.match(r'\d{4}', str(link.string)):
            url = link.get('href')
            # get quarter number by alphabet
            quarter = ord(url[-1]) - 96

            links.append({'year': int(link.string), 'quarter': quarter, 'url': f"https://www.cms.gov{url}"})

    # last 2 quarters
    links = sorted(links, key=lambda k: k['url'], reverse=True)[:2]

    for link in links:
        link['url'] = get_zip_file_url(link['url'])

    return links


def get_zip_file_url(url: str) -> Union[str, None]:
    res = requests.get(url)
    html_doc = res.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    for link in soup.find_all('a', href=True):
        zip_file_url = link.get('href')

        if '/files/zip/rvu' in zip_file_url:
            return f"https://www.cms.gov{zip_file_url}"

    return None


def download_files(**context) -> None:
    links: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_download_urls__task')
    folder_name = get_param('local_folder_name', **context)

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    for link in links:
        if link.get('url') is None:
            continue

        res = requests.get(link['url'])

        with open(f"{folder_name}/{link['year']}_{link['quarter']}.zip", 'wb') as file:
            file.write(res.content)

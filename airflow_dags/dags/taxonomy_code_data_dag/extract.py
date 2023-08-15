import os
import requests
from datetime import datetime
from typing import List

from bs4 import BeautifulSoup
from common.utils import get_param


def get_all_download_urls(**context) -> List[dict]:
    root_url = get_param('root_url', **context)
    res = requests.get(root_url)
    html_doc = res.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    links = []

    for link in soup.find_all('a', href=True):
        if 'CSV/nucc_taxonomy' in link['href']:
            date = str(link.string).split(' ')[-1]
            date = datetime.strptime(date, '%m/%d/%y').strftime('%y-%m-%d')
            url = f"https://www.nucc.org{link['href']}"

            links.append({'date': str(date), 'url': url})

    return links[:4]


def download_files(**context) -> None:
    links: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_download_urls__task')
    folder_name = get_param('local_folder_name', **context)

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    for link in links:
        if link.get('url') is None:
            continue

        res = requests.get(link['url'])

        with open(f"{folder_name}/{link['date']}.csv", 'wb') as file:
            file.write(res.content)

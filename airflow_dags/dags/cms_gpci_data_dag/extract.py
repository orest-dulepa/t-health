import os
import requests

from bs4 import BeautifulSoup
from common.utils import get_param


def get_all_download_urls(**context) -> str:
    root_url = get_param('root_url', **context)
    res = requests.get(root_url)
    soup = BeautifulSoup(res.content, 'html.parser')

    for link in soup.find_all('a', href=True):
        link_text = str(link.string).lower()

        if 'pfs final rule gpci' in link_text and 'zip' in link_text:
            return f"https://www.cms.gov{link['href']}"


def download_files(**context) -> None:
    url: str = context['ti'].xcom_pull(task_ids='main__get_all_download_urls__task')
    folder_name = get_param('local_folder_name', **context)

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

        res = requests.get(url)

        with open(f"{folder_name}/data.zip", 'wb') as file:
            file.write(res.content)

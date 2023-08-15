import logging
import re
from io import BytesIO
from pathlib import Path
from secrets import token_urlsafe
from tempfile import NamedTemporaryFile
from typing import Any
from typing import Dict
from typing import Pattern
from typing import cast
from urllib.parse import urlparse
from zipfile import ZipFile

import pandas as pd
import requests
import ujson as json
from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lxml import etree

from common.utils import get_param
from common.utils.xcom import get_return_value_from_xcom


logger = logging.getLogger(__name__)


def get_file_size(url: str) -> int:
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size


def get_file_url(**context: Dict[str, Any]) -> str:
    root_url = get_param('http_root_url', **context)
    xpath = get_param('file_xpath', **context)
    headers = {'Content-Type': 'text/html'}

    html = requests.get(root_url, headers=headers).text
    tree = etree.fromstring(html, etree.HTMLParser())
    try:
        element = tree.xpath(xpath)[0]
    except IndexError:
        raise AirflowException(f'Can not found any element using xpath: {xpath} url: {root_url}')

    file_name = element.attrib['href'].split('/', maxsplit=1)[-1]
    root_url_parsed = urlparse(root_url)
    path_parts = root_url_parsed.path.split('/')[:-1]
    path_parts.append(file_name)
    path = '/'.join(path_parts)

    return root_url_parsed._replace(path=path).geturl()


def search_file_in_zip(zip_file: ZipFile, pattern: Pattern) -> str:
    """
    Search file among other files in zip file using regex pattern
    :param zip_file: Zip file object
    :type zip_file: ZipFile
    :param pattern: regex patter for file name
    :type pattern: Pattern

    :return: srt

    """

    files = zip_file.namelist()

    logger.info(f'list available files in the container {files}')
    logger.info(f'pattern of csv: {pattern}')

    csv = next(filter(pattern.match, files))

    if not csv:
        raise AirflowException(f'Have no any files in zip files: {files}')

    return csv


def download_file(url: str, tmp_file: Path, chunk_size) -> None:
    """
    Download file as stream and write to disc space

    :param url: target url
    :type url: str
    :param tmp_file: file object
    :type tmp_file: NamedTemporaryFile
    :param chunk_size: Chunk size
    :type chunk_size: int

    """

    with requests.get(url, stream=True) as r, \
            open(tmp_file, 'wb') as tmp_file:
        logger.info(f'Opening stream from {url}')
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=chunk_size):
            tmp_file.write(chunk)
        logger.info(f'Content of {url} was successfully downloaded into {tmp_file.name}')


def download_zip(**context):
    """
    # Download zip from url to tmp


    """

    tmp_dir_path = get_return_value_from_xcom('set_up_tmp_folder_for_', **context)
    path = Path(tmp_dir_path).joinpath(token_urlsafe(3))
    logger.info(f'Path for zip {path}')
    url = get_file_url(**context)
    logger.info(f'URL is {url}')
    size = get_file_size(url)
    logger.info(f'Target size in bytes is {size}')
    chunk_size = get_param('chunk_size', **context)
    logger.info(f'Chunk size is {chunk_size}')
    logger.info(f'Amount of chunks is {size // chunk_size}')

    download_file(url, path, chunk_size)

    return str(path)


def get_csv_from_zip(**context):
    """

    :param context:
    :return:
    """
    pattern = re.compile(rf'{get_param("regex_pattern", **context)}')
    zip_file_path = get_return_value_from_xcom('download_zip_task', **context)

    with ZipFile(zip_file_path) as zip_f:
        logger.info(f'list available files in the container {zip_f.namelist()}')
        logger.info(f'Extract a specific file from the zip container - using pattern {pattern}')
        csv_file_name = search_file_in_zip(zip_f, pattern)
    return csv_file_name


def extract_metadata_from_csv(**context) -> str:
    """

    :param context:
    :return:
    """

    read_csv_conf = {
        'iterator': True,
        'chunksize': 1024 * 2,
        'parse_dates': True,
        'low_memory': True,
    }
    metadata = {}
    tmp_dir_path = get_return_value_from_xcom('set_up_tmp_folder_for_', **context)
    zip_file_path = get_return_value_from_xcom('download_zip_task', **context)
    csv_file_name = get_return_value_from_xcom('get_csv_from_zip_task', **context)
    s3_url = get_return_value_from_xcom('set_up_s3_bucket_for_', **context)
    s3_json_key = f'{s3_url}/{token_urlsafe(3)}.json'

    with ZipFile(zip_file_path) as zip_f, \
            NamedTemporaryFile(dir=tmp_dir_path) as tmp_file_for_csv, \
            zip_f.open(csv_file_name) as csv_file:

        tf_reader = pd.read_csv(csv_file, **read_csv_conf)
        first_chunk = next(tf_reader)
        for column, data_type in zip(first_chunk.keys(),
                                     map(lambda x: str(first_chunk.dtypes[x]), first_chunk.keys())):
            metadata[column] = {data_type: 1}

        for df in tf_reader:
            for column, data_type in zip(df.keys(),
                                         map(lambda x: str(df.dtypes[x]), df.keys())):
                if data_type in metadata[column]:
                    metadata[column][data_type] += 1
                else:
                    metadata[column][data_type] = 1

        data = json.dumps(metadata).encode()
        tmp_file_for_csv.write(data)
        tmp_file_for_csv.seek(0)
        S3Hook().load_file_obj(tmp_file_for_csv, key=s3_json_key, replace=True)
    return s3_json_key


def extract_csv_from_zip_to_s3(**context) -> str:
    """
    # Extract csv from zip and save to s3 tmp folder

    """

    zip_file_path = get_return_value_from_xcom('download_zip_task', **context)
    csv_file_name = get_return_value_from_xcom('get_csv_from_zip_task', **context)

    s3_url = get_return_value_from_xcom('set_up_s3_bucket_for_', **context)
    s3_csv_key = f'{s3_url}/{token_urlsafe(3)}.csv'

    with ZipFile(zip_file_path) as zip_f, \
            zip_f.open(csv_file_name) as f:
        S3Hook().load_file_obj(
            file_obj=cast(BytesIO, f),
            key=s3_csv_key,
            replace=True,
        )
    return s3_csv_key

import logging
from tempfile import mkdtemp
from typing import Any
from typing import Dict

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.utils import get_param

logger = logging.getLogger(__name__)


def set_up_tmp_folder(**context: Dict[str, Any]) -> str:
    """
    # Create temporary folder

    """

    dag_id = context["dag"].dag_id
    logger.info(f'Create tmp folder for {dag_id}')
    tmp_dir = mkdtemp(prefix=f'{dag_id}_')

    return tmp_dir


def check_url(**context: Dict[str, Any]) -> None:
    """
    # Check if url is reachable

    """

    url = get_param('http_root_url', **context)

    logger.info(f'Check if url {url} is reachable')
    requests.head(url).raise_for_status()
    logger.info(f'Url {url} - Ok')


def check_db(**context: Dict[str, Any]) -> None:
    """
    # Check connection to a database

    """

    db_conn_name = get_param('db_conn_name', **context)

    logger.info(f'Check if db connection {db_conn_name} is exists')
    hook = PostgresHook.get_hook(db_conn_name)
    logger.info(f'Get connection for {db_conn_name}, hook {hook}')

import logging
import shutil
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional

import requests
from airflow.models import DAG
from airflow.models import DagRun

logger = logging.getLogger(__name__)


def apply_readme(path: Path, dag: DAG, default: str = '') -> None:
    """
    Get content from REAMDE file in dag root directory and apply it to doc_md section.
    :param path: Absolute path to README file
    :param dag: Airflow Dag instance
    :param default: Some default value
    :return: None
    """

    if path.exists():
        with open(path, 'r') as doc:
            dag.doc_md = doc.read()
    else:
        dag.doc_md = default


def get_param(key: str, dag_run: DagRun, params: Dict[str, Any], **context: Dict[str, Any]) -> Any:
    """
    Get dag param from runtime config or params.
    In case when dag is running under config (usual that's a manual triggering)
    params will be fetched from dar_run.conf otherwise from context['params']

    :param key: name of parameter that should be fetched
    :param dag_run: dar_run instance from context
    :param params: params dict from context
    :param context: context object. Not used yet.
    :return:
    """

    runtime_conf = dag_run.conf
    if runtime_conf:
        return runtime_conf.get(key, params[key])
    return params[key]


def get_return_value_from_xcom(task_id: str, **context) -> Optional[str]:
    """

    :param task_id:
    :param context:
    :return:

    """

    dag_id = context["dag"].dag_id

    if '_for_' in task_id:
        task_id = f'{task_id}{dag_id}'

    try:
        return context['ti'].xcom_pull(
            key='return_value',
            task_ids=(task_id,),
            dag_id=dag_id)[0]
    except KeyError:
        return None


def check_url(**context) -> None:
    """
    # Check if url is reachable

    """

    root_url = get_param('root_url', **context)

    logger.info(f'Check if url {root_url} is reachable')
    requests.head(root_url).raise_for_status()
    logger.info(f'Url {root_url} - Ok')


def clean_up(**context: Dict[str, Any]):
    logger.info('Cleanup')

    folder_name = get_param('local_folder_name', **context)
    shutil.rmtree(folder_name)

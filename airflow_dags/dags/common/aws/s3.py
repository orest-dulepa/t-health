import logging
from typing import Any
from typing import Dict

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common.utils import get_param

logger = logging.getLogger(__name__)


def set_up_s3_bucket(**context: Dict[str, Any]) -> str:
    """
    # Check if s3 root bucket is reachable
        and create root url for tmp bucket
    """

    s3 = S3Hook()
    s3_root_url = get_param('s3_root_url', **context)
    bucket, key = s3.parse_s3_url(s3_root_url)

    logger.info('Check s3 bucket %s is reachable', bucket)
    if not s3.check_for_bucket(bucket):
        raise AirflowException(
            f'S3 bucket {bucket} does not exists or unreachable.')

    dag_id = context["dag"].dag_id
    s3_tmp_bucket_url = f's3://{bucket}/{key}/{dag_id}'
    logger.info('S3 bucket %s - Ok', bucket)
    return s3_tmp_bucket_url

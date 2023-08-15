import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common.utils.xcom import get_return_value_from_xcom
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def clean_up_s3_bucket(**context) -> None:
    """
    Clean up s3 bucket
    """
    s3_bucket_url = get_return_value_from_xcom(
        'set_up_s3_bucket_for_', **context)
    logger.info('Clean up s3 bucket: %s', s3_bucket_url)

    s3 = S3Hook()
    bucket, prefix = s3.parse_s3_url(s3_bucket_url)
    objects_to_delete = s3.conn.list_objects(Bucket=bucket, Prefix=prefix)
    delete_keys = [
        obj['Key'] for obj in objects_to_delete.get('Contents', [])]

    if delete_keys:
        bucket = s3.get_bucket(bucket_name=bucket)
        for object_key in delete_keys:
            try:
                bucket.object_versions.filter(Prefix=object_key).delete()
                logger.info(
                    "Permanently deleted all versions of object %s.",
                    object_key)
            except ClientError:
                logger.exception(
                    "Couldn't delete all versions of %s.",
                    object_key)
                raise
        logger.info('%s objects has been deleted', len(delete_keys))

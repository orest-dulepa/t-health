from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from contextlib import closing
from functools import partial
from io import BytesIO
from itertools import islice
from logging import getLogger
from os import SEEK_SET
from re import compile
from re import sub

import ujson as json
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.exceptions import ClientError

from common.utils.xcom import get_return_value_from_xcom, get_range

from common.utils import get_param

logger = getLogger(__name__)

DATA_TYPE_MAPPING = {
    'int64': 'int8',
    'float64': 'float8',
    'object': 'varchar',
}
TMP_PREFIX = '_tmp'


def create_db_table(**context):
    """

    :param context:
    :return:
    """
    data = {}
    pattern = compile(r"[^0-9a-zA-Z]+")
    schema = 'internal_reference'
    table_name = 'ref_cms_nppes_npi'
    table_full_name = f'{schema}.{table_name}{TMP_PREFIX}'
    s3_url = get_return_value_from_xcom('extract_metadata_from_csv_task', **context)
    conn_name = Variable.get('nppes_data_db_conn')
    hook = PostgresHook(postgres_conn_id=conn_name)

    with open(S3Hook().download_file(s3_url)) as f:
        metadata = json.load(f)

    for column in metadata.keys():
        # ToDo: Move it to extract_metadata_from_csv and refactor
        formatted_column_name = sub(pattern, '_', column).lower().rstrip('_')[:59]
        if 'date' in formatted_column_name:
            t = 'date'
        elif 'license_number' in formatted_column_name:
            t = 'varchar'
        elif 'identifier' in formatted_column_name:
            t = 'varchar'
        elif 'text' in formatted_column_name:
            t = 'varchar'
        elif 'code' in formatted_column_name:
            t = 'varchar'
        elif 'group' in formatted_column_name:
            t = 'varchar'
        elif 'telephone_number' in formatted_column_name:
            t = 'varchar'
        elif 'fax_number' in formatted_column_name:
            t = 'varchar'
        elif formatted_column_name == 'healthcare_provider_primary_taxonomy_switch_15':
            t = 'varchar'
        else:
            t = DATA_TYPE_MAPPING[max(metadata[column], key=metadata[column].get)]
        data[formatted_column_name] = t

    sql = f"""
        drop table if exists {table_full_name};
        create table {table_full_name} (
        {",".join(map(lambda item: f"{item[0]} {item[1]}", data.items()))}
        );
    """
    hook.run(sql)

    return table_full_name


def update_db_table(**context):
    """

    :param context:
    :return:

    """

    conn_name = Variable.get('nppes_data_db_conn')
    hook = PostgresHook(postgres_conn_id=conn_name)
    table_name_tmp = get_return_value_from_xcom('prepare_db_before_uploading_task', **context)
    schema, table_name = table_name_tmp.split('.', maxsplit=1)
    table_name = table_name.rstrip(TMP_PREFIX)

    logger.info(f'Drop table {schema}.{table_name}')
    logger.info(f'Rename table "{table_name_tmp}" to "{table_name}"')

    hook.run(f"""
        drop table if exists {schema}.{table_name};
        alter table {table_name_tmp} rename to {table_name};
    """, autocommit=True)


def get_s3_file_size(s3, bucket: str, key: str) -> int:
    """Gets the file size of S3 object by a HEAD request

    Args:
        bucket (str): S3 bucket
        key (str): S3 object path

    Returns:
        int: File size in bytes. Defaults to 0 if any error.
    """
    file_size = 0
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        if response:
            file_size = int(response.get('ResponseMetadata').get('HTTPHeaders').get('content-length'))
    except ClientError:
        logger.exception(f'Client error reading S3 file {bucket} : {key}')
    return file_size


def stream_and_save(s3, bucket: str, key: str, db_conn: str, sql: str, start_range: int, end_range: int) -> None:
    expression = 'SELECT * FROM S3Object'
    result_stream = []

    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={
            'CSV': {
                'FileHeaderInfo': 'IGNORE',
            }
        },
        OutputSerialization={
            'CSV': {}
        },
        ScanRange={
            'Start': start_range,
            'End': end_range
        },
    )

    for event in response['Payload']:
        records = event.get('Records')
        if records:
            result_stream.append(records['Payload'])

    del response
    if not result_stream:
        return

    buffer = BytesIO()
    buffer.write(b''.join(result_stream))
    buffer.seek(SEEK_SET)
    result_stream.clear()

    with closing(PostgresHook(postgres_conn_id=db_conn).get_conn()) as db_conn, \
            closing(db_conn.cursor()) as cursor:
        try:
            cursor.copy_expert(sql, buffer)
            db_conn.commit()
        except Exception:
            logger.exception(f'Got Exception')


def upload_csv_from_s3_to_postgres(**context):
    """

    :param context:
    :return:

    """

    table_name = get_return_value_from_xcom('prepare_db_before_uploading_task', **context)
    s3_csv_url = get_return_value_from_xcom('upload_csv_to_s3_task', **context)

    bucket, key = S3Hook.parse_s3_url(s3_csv_url)
    s3_client = S3Hook().get_client_type('s3')
    conn_name = Variable.get('nppes_data_db_conn')

    file_size = get_s3_file_size(s3_client, bucket=bucket, key=key)
    chunk_size = get_param('chunk_size', **context)
    how_many_tasks_at_once = get_param('how_many_tasks_at_once', **context)

    sql = f"COPY {table_name} FROM STDIN WITH (DELIMITER ',', NULL '', FORMAT CSV, HEADER FALSE);"
    func = partial(stream_and_save, s3=s3_client, bucket=bucket, key=key, db_conn=conn_name, sql=sql)

    logger.info(f'Approximately amount of chunks is: {file_size // chunk_size}')
    _range = iter(get_range(file_size, chunk_size))

    with ThreadPoolExecutor(max_workers=how_many_tasks_at_once + 1) as executor:
        futures = {
            executor.submit(func, start_range=_r[0], end_range=_r[1]): repr(_r)
            for _r in islice(_range, how_many_tasks_at_once)
        }

        while futures:
            # Wait for the next future to complete.
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            len_done = len(done)
            for fut in done:
                futures.pop(fut)  # remove done futures from the pool

            # Schedule the next set of futures.
            # We don't want more than N futures in the pool at a time
            # to keep memory consumption down.
            for _r in islice(_range, len_done):
                logger.info(f'Scheduled new {len_done} tasks')
                fut = executor.submit(func, start_range=_r[0], end_range=_r[1])
                futures[fut] = repr(_r)

            logger.info(f'Last chunk size: {_r}')

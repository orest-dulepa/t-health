"""Refresh NPI information for all US providers"""

from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.utils import apply_readme
from common.aws.s3 import set_up_s3_bucket
from common.aws.cleanup import clean_up_s3_bucket
from .cleanup import clean_up_tmp_dir
from .extract import download_zip
from .extract import extract_csv_from_zip_to_s3
from .extract import extract_metadata_from_csv
from .extract import get_csv_from_zip
from .load import create_db_table
from .load import update_db_table
from .load import upload_csv_from_s3_to_postgres
from .notify import send_slack_failure
from .notify import send_slack_success
from .setup import check_url, check_db
from .setup import set_up_tmp_folder

DEFAULT_ARGS = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(3),
    'provide_context': True,
    'email': Variable.get(key="admin_emails", default_var=[], deserialize_json=True),
    'email_on_failure': Variable.get(key="send_email_on_failure", default_var=False, deserialize_json=True),
    'email_on_retry': Variable.get(key="send_admin_emails", default_var=False, deserialize_json=True),
}
with DAG(
        dag_id='nppes_etl_data_processing',
        description=__doc__,
        default_args=DEFAULT_ARGS,
        schedule_interval='@monthly',
        catchup=False,
        tags=[
            'th',
            'data-team',
            'nppes',
        ],
        params={
            'http_root_url': Variable.get(key='nppes_data_root_url', default_var=''),
            'file_xpath': Variable.get(key='nppes_data_csv_file_xpath', default_var=''),
            's3_root_url': Variable.get(key='aws_s3_bucket', default_var=''),
            'db_conn_name': Variable.get(key='nppes_data_db_conn', default_var=''),
            'chunk_size': Variable.get(key='chunk_size', default_var=5_120_000),  # default is 5.12MB
            'regex_pattern': Variable.get(key='npidata_filename_regex', default_var=''),
            'how_many_tasks_at_once': Variable.get(key='how_many_tasks_at_once', default_var=8)
        },
) as dag:
    # region doc
    readme = Path.joinpath(Path(__file__).parent, 'README.md')
    apply_readme(readme, dag, __doc__)
    # endregion

    # region setup
    setup__s3_bucket__task = PythonOperator(
        task_id=f'set_up_s3_bucket_for_{dag.dag_id}',
        python_callable=set_up_s3_bucket,
        doc_md=set_up_s3_bucket.__doc__,
        provide_context=True,
    )
    setup__tmp_folder__task = PythonOperator(
        task_id=f'set_up_tmp_folder_for_{dag.dag_id}',
        python_callable=set_up_tmp_folder,
        doc_md=set_up_tmp_folder.__doc__,
        provide_context=True,
    )
    setup__check_target_url__task = PythonOperator(
        task_id='check_if_url_available',
        python_callable=check_url,
        doc_md=check_url.__doc__,
        provide_context=True,
    )
    setup__check_db_conn__task = PythonOperator(
        task_id=f'check_db_conn_for_{dag.dag_id}',
        python_callable=check_db,
        doc_md=check_db.__doc__,
        provide_context=True,
    )
    # endregion

    # region main
    dummy_start = DummyOperator(task_id=f'Start_pipeline_for_{dag.dag_id}',
                                trigger_rule=TriggerRule.DUMMY)
    dummy_done = DummyOperator(task_id=f'Finish_pipeline_for_{dag.dag_id}',
                               trigger_rule=TriggerRule.ALL_DONE)

    download_zip_task = PythonOperator(
        task_id='download_zip_task',
        python_callable=download_zip,
        doc_md=download_zip.__doc__,
        provide_context=True,
    )
    get_csv_from_zip_task = PythonOperator(
        task_id='get_csv_from_zip_task',
        python_callable=get_csv_from_zip,
        doc_md=get_csv_from_zip.__doc__,
        provide_context=True,
    )
    upload_csv_to_s3_task = PythonOperator(
        task_id='upload_csv_to_s3_task',
        python_callable=extract_csv_from_zip_to_s3,
        doc_md=extract_csv_from_zip_to_s3.__doc__,
        provide_context=True,
    )
    extract_metadata_from_csv_task = PythonOperator(
        task_id='extract_metadata_from_csv_task',
        python_callable=extract_metadata_from_csv,
        doc_md=extract_metadata_from_csv.__doc__,
        provide_context=True,
    )
    prepare_db_before_uploading_task = PythonOperator(
        task_id='prepare_db_before_uploading_task',
        python_callable=create_db_table,
        doc_md=create_db_table.__doc__,
        provide_context=True,
    )
    upload_csv_from_s3_to_postgres_task = PythonOperator(
        task_id='upload_csv_from_s3_to_postgres_task',
        python_callable=upload_csv_from_s3_to_postgres,
        doc_md=upload_csv_from_s3_to_postgres.__doc__,
        provide_context=True,
    )
    prepare_db_after_uploading_task = PythonOperator(
        task_id='prepare_db_after_uploading_task',
        python_callable=update_db_table,
        doc_md=update_db_table.__doc__,
        provide_context=True,
    )
    notify_success_task = PythonOperator(
        task_id='notify_slack_success_task',
        python_callable=send_slack_success,
        doc_md=send_slack_success.__doc__,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True,

    )
    notify_failure_task = PythonOperator(
        task_id='notify_slack_failure_task',
        python_callable=send_slack_failure,
        doc_md=send_slack_failure.__doc__,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )
    # endregion

    # region cleanup

    clean_up__tmp_dir__task = PythonOperator(
        task_id=f'clean_up_tmp_folder_for_{dag.dag_id}',
        python_callable=clean_up_tmp_dir,
        doc_md=clean_up_tmp_dir.__doc__,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )
    clean_up__s3_bucket__task = PythonOperator(
        task_id=f'clean_up_s3_bucket_for_{dag.dag_id}',
        python_callable=clean_up_s3_bucket,
        doc_md=clean_up_s3_bucket.__doc__,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )
    # endregion

    # region relationship
    setup = (setup__s3_bucket__task,
             setup__tmp_folder__task,
             setup__check_target_url__task,
             setup__check_db_conn__task)
    cleanup = (clean_up__tmp_dir__task,
               clean_up__s3_bucket__task,
               notify_success_task,
               notify_failure_task)
    extract = (upload_csv_to_s3_task,
               extract_metadata_from_csv_task)

    (
            dummy_start >>
            setup >>
            download_zip_task >>
            get_csv_from_zip_task >>
            extract >>
            prepare_db_before_uploading_task >>
            upload_csv_from_s3_to_postgres_task >>
            prepare_db_after_uploading_task >>
            cleanup >>
            dummy_done
    )
    # endregion

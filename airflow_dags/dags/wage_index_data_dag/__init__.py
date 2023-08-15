"""Refresh Wage Index Tables Final Rule and Correction Notice"""

from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.utils import apply_readme
from .cleanup import clean_up
from .extract import download_wage_index_files, get_all_download_urls
from .load import load_wage_indexes_to_db
from .notify import send_slack_failure
from .notify import send_slack_success
from .setup import check_url, create_table_if_not_exists
from .transform import get_all_wage_indexes

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
        dag_id='wage_index_etl_data_processing',
        description=__doc__,
        default_args=DEFAULT_ARGS,
        schedule_interval='@yearly',
        catchup=False,
        tags=[
            'th',
            'data-team',
            'wage-index',
        ],
        params={
            'local_folder_name': Variable.get(key='local_folder_name', default_var='wage_indexes'),
        },
) as dag:
    # region doc
    readme = Path.joinpath(Path(__file__).parent, 'README.md')
    apply_readme(readme, dag, __doc__)
    # endregion

    # region setup
    setup__create_table_if_not_exists__task = PythonOperator(
        task_id='setup__create_table_if_not_exists__task',
        python_callable=create_table_if_not_exists,
        doc_md=create_table_if_not_exists.__doc__,
    )

    setup__check_target_url__task = PythonOperator(
        task_id='setup__check_target_url__task',
        python_callable=check_url,
        doc_md=check_url.__doc__,
        provide_context=True,
    )
    # endregion

    # region main
    main__get_all_download_urls__task = PythonOperator(
        task_id='main__get_all_download_urls__task',
        python_callable=get_all_download_urls,
        doc_md=get_all_download_urls.__doc__,
    )

    main__download_files__task = PythonOperator(
        task_id='main__download_files__task',
        python_callable=download_wage_index_files,
        doc_md=download_wage_index_files.__doc__,
    )

    main__get_all_wage_indexes__task = PythonOperator(
        task_id='main__get_all_wage_indexes__task',
        python_callable=get_all_wage_indexes,
        doc_md=get_all_wage_indexes.__doc__,
    )

    main__load_wage_indexes_to_db__task = PythonOperator(
        task_id='main__load_wage_indexes_to_db__task',
        python_callable=load_wage_indexes_to_db,
        doc_md=load_wage_indexes_to_db.__doc__,
    )
    # endregion

    # region teardown
    teardown__cleanup__task = PythonOperator(
        task_id='teardown__cleanup__task',
        python_callable=clean_up,
        doc_md=clean_up.__doc__,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # endregion

    # region notify
    notify__slack_success__task = PythonOperator(
        task_id='notify__slack_success__task',
        python_callable=send_slack_success,
        doc_md=send_slack_success.__doc__,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify__slack_failure__task = PythonOperator(
        task_id='notify__slack_failure__task',
        python_callable=send_slack_failure,
        doc_md=send_slack_failure.__doc__,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    # endregion

    # region relationship
    [
        setup__create_table_if_not_exists__task,
        setup__check_target_url__task
    ] >> main__get_all_download_urls__task >> main__download_files__task >> main__get_all_wage_indexes__task >> \
    main__load_wage_indexes_to_db__task >> teardown__cleanup__task >> [
        notify__slack_success__task,
        notify__slack_failure__task
    ]
    # endregion

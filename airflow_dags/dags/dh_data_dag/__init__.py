"""Download and process Definitive Healthcare sftp files"""
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from dh_data_dag.operators import DHDownloadOperator
from dh_data_dag.load import load_data_to_db, truncate_output_tables
from common.utils import apply_readme
from common.aws.s3 import set_up_s3_bucket
from common.aws.cleanup import clean_up_s3_bucket
from common.slack.notify import send_slack_failure, send_slack_success


DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": days_ago(3),
    "provide_context": True,
    "email": Variable.get(
        key="admin_emails", default_var=[], deserialize_json=True),
    "email_on_failure": Variable.get(
        key="send_email_on_failure", default_var=False, deserialize_json=True),
    "email_on_retry": Variable.get(
        key="send_admin_emails", default_var=False, deserialize_json=True),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


TMP_DIR = "/tmp/data/dh/{{ run_id }}"


with DAG(
        dag_id="dh_etl_data_processing",
        description=__doc__,
        default_args=DEFAULT_ARGS,
        schedule_interval="0 0 11 1,4,7,10 *",  # At 00:00 on 11 day of each quarter
        catchup=False,
        tags=[
            "th",
            "data-team",
            "dh",
        ],
        params={
            "s3_root_url": Variable.get(key='aws_s3_bucket', default_var=''),
        }

) as dag:
    readme = Path.joinpath(Path(__file__).parent, "README.md")
    apply_readme(readme, dag, __doc__)
    ssh_conn_id = Variable.get("dh_sftp_server", default_var="dh_sftp_server")

    dummy_start = DummyOperator(
        task_id=f"Start_pipeline_for_{dag.dag_id.upper()}")
    dummy_done = DummyOperator(
        task_id=f"Finish_pipeline_for_{dag.dag_id.upper()}",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    setup__s3_bucket__task = PythonOperator(
        task_id=f'set_up_s3_bucket_for_{dag.dag_id}',
        python_callable=set_up_s3_bucket,
        doc_md=set_up_s3_bucket.__doc__,
        provide_context=True,
    )

    download___surgeries___task = DHDownloadOperator(
        task_id="get-surgeries",
        ssh_conn_id=ssh_conn_id,
        remote_filepath="/Surgery_Centers/Surgery_Centers_Quarterly.zip",
        local_filepath=f"{TMP_DIR}/Surgery_Centers_Quarterly.zip",
        operation="get",
        create_intermediate_dirs=True,
    )

    download___hospitals___task = DHDownloadOperator(
        task_id="get-hospitals",
        ssh_conn_id=ssh_conn_id,
        remote_filepath="/Hospitals/Hospitals_Quarterly.zip",
        local_filepath=f"{TMP_DIR}/Hospitals_Quarterly.zip",
        operation="get",
        create_intermediate_dirs=True,
    )

    download___physicians___task = DHDownloadOperator(
        task_id="get-physicians",
        ssh_conn_id=ssh_conn_id,
        remote_filepath="/Physicians/Physicians_Quarterly.zip",
        local_filepath=f"{TMP_DIR}/Physicians_Quarterly.zip",
        operation="get",
        create_intermediate_dirs=True)

    download___physician_groups___task = DHDownloadOperator(
        task_id="get-physician-groups",
        ssh_conn_id=ssh_conn_id,
        remote_filepath="/Physician_Groups/Physician_Groups_Quarterly.zip",
        local_filepath=f"{TMP_DIR}/Physician_Groups_Quarterly.zip",
        operation="get",
        create_intermediate_dirs=True)

    download___img_centers___task = DHDownloadOperator(
        task_id="get-img-centers",
        ssh_conn_id=ssh_conn_id,
        remote_filepath="/Imaging_Centers/Imaging_Centers_Quarterly.zip",
        local_filepath=f"{TMP_DIR}/Imaging_Centers_Quarterly.zip",
        operation="get",
        create_intermediate_dirs=True)

    truncate__dh_tables__task = PythonOperator(
        task_id=f"truncate_tables_for_{dag.dag_id}",
        python_callable=truncate_output_tables,
        doc_md=truncate_output_tables.__doc__,
        provide_context=True, )

    load__dh_files__task = PythonOperator(
        task_id=f"load_files_for_{dag.dag_id}",
        python_callable=load_data_to_db,
        doc_md=load_data_to_db.__doc__,
        provide_context=True,
        op_kwargs={"data_dir": TMP_DIR})

    clean__dh_tmp_files__task = BashOperator(
        task_id=f"cleanup_tmp_files_for_{dag.dag_id}",
        bash_command=f"rm -rf {TMP_DIR}",
    )

    clean_up_s3_bucket__task = PythonOperator(
        task_id=f'clean_up_s3_bucket_for_{dag.dag_id}',
        python_callable=clean_up_s3_bucket,
        doc_md=clean_up_s3_bucket.__doc__,
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

    download = (
        download___surgeries___task,
        download___hospitals___task,
        download___physicians___task,
        download___physician_groups___task,
        download___img_centers___task,
    )
    cleanup = (
        clean__dh_tmp_files__task,
        clean_up_s3_bucket__task,
        notify_success_task,
        notify_failure_task
    )

    (
        dummy_start >>
        setup__s3_bucket__task >>
        download >>
        truncate__dh_tables__task >>
        load__dh_files__task >>
        cleanup >>
        dummy_done
    )

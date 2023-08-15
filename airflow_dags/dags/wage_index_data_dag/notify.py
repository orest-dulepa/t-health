from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common.enums import StateEnum

__all__ = [
    'send_slack_success',
    'send_slack_failure',
]


def send_slack_success(**context):
    """
    Notify in #data slack channel about success
    """
    context['state'] = StateEnum.SUCCESS.name
    return _task_slack_alert(**context)


def send_slack_failure(**context):
    """
    Notify in #data slack channel about failure
    """
    print(f'From Notify {__file__} file')
    context['state'] = StateEnum.FAILURE.name
    return _task_slack_alert(**context)


def _task_slack_alert(**context):
    slack_alert_conn_name = Variable.get(key='slack_alert')  # get connection name here
    state = context['state']  # get state here
    slack_webhook_token = BaseHook.get_connection(slack_alert_conn_name).password
    task = context.get('task_instance')
    slack_msg = (
        f""":{state}:
    *Task*: {task.task_id}
    *Dag*: {task.dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {task.log_url})"""
    )

    alert = SlackWebhookOperator(
        task_id=f'task_slack_alert_{state}',
        http_conn_id=slack_alert_conn_name,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)

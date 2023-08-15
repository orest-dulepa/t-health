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
    context['state'] = StateEnum.FAILURE.name
    return _task_slack_alert(**context)


def _task_slack_alert(state, **context):
    slack_http_conn_id = Variable.get(key='slack_alert')
    slack_webhook_token = BaseHook.get_connection(slack_http_conn_id).password
    task = context.get('task_instance')
    emoji = ':large_green_circle:' if state == StateEnum.SUCCESS.name else ':red_circle:'
    slack_msg = (
        f""":{emoji}:
    *Task*: {task.task_id}
    *Dag*: {task.dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {task.log_url})"""
    )

    alert = SlackWebhookOperator(
        task_id=f'task_slack_alert_{state}',
        http_conn_id=slack_http_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)

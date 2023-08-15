import os
import celery
import pybrake
from pybrake.celery import patch_celery
from mpfs_pricer import pricer

app = celery.Celery('mpfs_pricer')

app.conf.update(
    broker_url=os.environ.get('REDIS_URL', 'redis://localhost:6379'),
    result_backend=os.environ.get('REDIS_URL', 'redis://localhost:6379'),
    task_default_queue="mpfs",
)

airbrake_project_id = os.environ.get('AIRBRAKE_PROJECT_ID', '')
airbrake_project_key = os.environ.get('AIRBRAKE_PROJECT_KEY', '')
if airbrake_project_id != '' and airbrake_project_key != '':
    notifier = pybrake.Notifier(project_id=airbrake_project_id,
                                project_key=airbrake_project_key,
                                environment="production")
    patch_celery(notifier)


@app.task
def price_claim_data(data):
    return [pricer.price_claim(claim) for claim in data]

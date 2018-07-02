import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.utils import helpers
from .get_recent_job import get_recent_job

def stop_app(spark_job_operations, job_id, application_name):
    recent_run_job = get_recent_job(spark_job_operations, job_id)

    # stop batch task
    try:
        spark_job_operations.batch_client.task.terminate(job_id=recent_run_job.id, task_id=application_name)
        return True
    except batch_error.BatchErrorException:
        return False

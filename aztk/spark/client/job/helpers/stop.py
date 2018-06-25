import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.utils import helpers

from .get_recent_job import get_recent_job


def _stop(spark_client, job_id):
    # terminate currently running job and tasks
    recent_run_job = get_recent_job(spark_client, job_id)
    spark_client.batch_client.job.terminate(recent_run_job.id)
    # terminate job_schedule
    spark_client.batch_client.job_schedule.terminate(job_id)


def stop(self, job_id):
    try:
        return _stop(self, job_id)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

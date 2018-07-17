import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.utils import helpers


def _list_jobs(spark_job_operations):
    return [cloud_job_schedule for cloud_job_schedule in spark_job_operations.batch_client.job_schedule.list()]


def list_jobs(self):
    try:
        return [models.Job(cloud_job_schedule) for cloud_job_schedule in _list_jobs(self)]
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

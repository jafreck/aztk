import azure.batch.models.batch_error as batch_error

import aztk.models    # TODO: get rid of this import and use aztk.spark.models
from aztk import error
from aztk.spark import helpers, models


def _list_jobs(spark_client):
    return [cloud_job_schedule for cloud_job_schedule in spark_client.batch_client.job_schedule.list()]


def list_jobs(self):
    try:
        return [models.Job(cloud_job_schedule) for cloud_job_schedule in _list_jobs(self)]
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

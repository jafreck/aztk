from azure.batch.models import BatchErrorException

from aztk import error
from aztk.spark import models
from aztk.utils import helpers


def _list_jobs(core_job_operations):
    return [cloud_job_schedule for cloud_job_schedule in core_job_operations.batch_client.job_schedule.list()]


def list_jobs(core_job_operations):
    from aztk.utils import batch_error_manager
    with batch_error_manager():
        return [models.Job(cloud_job_schedule) for cloud_job_schedule in _list_jobs(core_job_operations)]

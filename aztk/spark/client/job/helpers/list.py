from azure.batch.models import BatchErrorException

from aztk import error
from aztk.spark import models
from aztk.utils import helpers


def filter_aztk_jobs(jobs):
    #TODO: filter by metadata
    return jobs


def list_jobs(core_job_operations):
    from aztk.utils import batch_error_manager
    with batch_error_manager():
        return [models.Job(job) for job in filter_aztk_jobs(core_job_operations.batch_client.job.list())]

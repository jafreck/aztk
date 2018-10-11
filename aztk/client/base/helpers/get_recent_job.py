from azure.batch.models import batch_error

from aztk import error
from aztk.utils import helpers


# Note: this only works with jobs, not clusters
# cluster impl is planned to change to job schedule
def get_recent_job(core_job_operations, id):
    try:
        job_schedule = core_job_operations.batch_client.job_schedule.get(id)
        return core_job_operations.batch_client.job.get(job_schedule.execution_info.recent_job.id)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

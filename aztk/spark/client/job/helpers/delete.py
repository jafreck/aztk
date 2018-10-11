import azure.batch.models as batch_models
from azure.batch.models import BatchErrorException

from aztk import error
from aztk.utils import helpers


def _delete(core_job_operations, spark_job_operations, job_id, keep_logs: bool = False):
    recent_run_job = core_job_operations.get_recent_job(job_id)
    deleted_job_or_job_schedule = False
    # delete job
    try:
        core_job_operations.batch_client.job.delete(recent_run_job.id)
        deleted_job_or_job_schedule = True
    except batch_models.BatchErrorException:
        pass
    # delete job_schedule
    try:
        core_job_operations.batch_client.job_schedule.delete(job_id)
        deleted_job_or_job_schedule = True
    except batch_models.BatchErrorException:
        pass

    # delete storage container
    if keep_logs:
        cluster_data = core_job_operations.get_cluster_data(job_id)
        cluster_data.delete_container(job_id)

    table_exists = core_job_operations.table_service.exists(job_id)
    if table_exists:
        core_job_operations.delete_task_table(job_id)

    return deleted_job_or_job_schedule


def delete(core_job_operations, spark_job_operations, job_id: str, keep_logs: bool = False):
    try:
        return _delete(core_job_operations, spark_job_operations, job_id, keep_logs)
    except BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

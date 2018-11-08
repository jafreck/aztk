from azure.batch.models import BatchErrorException
from msrest.exceptions import ClientRequestError

from aztk.utils import BackOffPolicy, helpers, retry


@retry(retry_count=4, retry_interval=1, backoff_policy=BackOffPolicy.exponential, exceptions=(ClientRequestError))
def delete_batch_resources(core_base_operations, job_id, keep_logs: bool = False):
    success = False

    # delete batch job, autopool
    try:
        core_base_operations.batch_client.job.delete(job_id)
        success = True
    except BatchErrorException:
        pass

    # delete storage container
    if not keep_logs:
        cluster_data = core_base_operations.get_cluster_data(job_id)
        cluster_data.delete_container(job_id)
        success = True

    table_exists = core_base_operations.table_service.exists(job_id)
    if table_exists:
        core_base_operations.delete_task_table(job_id)
        success = True

    return success

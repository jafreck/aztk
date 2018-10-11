from azure.batch.models import BatchErrorException

from aztk import error
from aztk.models import TaskState
from aztk.utils import helpers


def get_application_status(core_cluster_operations, cluster_id: str, app_name: str):
    try:
        return core_cluster_operations.get_task_status(cluster_id, app_name)
    except BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

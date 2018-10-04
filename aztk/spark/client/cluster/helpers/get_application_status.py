import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.models import TaskState
from aztk.utils import helpers


def get_application_status(core_cluster_operations, cluster_id: str, app_name: str):
    try:
        return core_cluster_operations.get_task_status(cluster_id, app_name)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

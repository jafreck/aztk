import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.models import TaskState
from aztk.utils import helpers


def get_task_status(core_cluster_operations, cluster_id: str, task_id: str):
    try:
        # TODO: return TaskState object instead of str
        scheduling_target = core_cluster_operations.get_cluster_configuration(cluster_id).scheduling_target
        if scheduling_target:
            # get storage table entry for this application
            # convert entry to application_status
            entity = core_cluster_operations.get_task_from_table(cluster_id, task_id)
            return TaskState(entity.state).value
        else:
            task = core_cluster_operations.batch_client.task.get(cluster_id, task_id)
        return task.state.name
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

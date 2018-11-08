from aztk.models import SchedulingTarget
from aztk.utils import batch_error_manager


def get_task_state(core_cluster_operations, cluster_id: str, task_id: str):
    with batch_error_manager():
        task = core_cluster_operations.get_task(cluster_id, task_id)
        return task.state

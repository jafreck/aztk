from aztk.models import SchedulingTarget
from aztk.utils import batch_error_manager


def get_task_state(core_cluster_operations, cluster_id: str, task_id: str):
    with batch_error_manager():
        scheduling_target = core_cluster_operations.get_cluster_configuration(cluster_id).scheduling_target
        if scheduling_target is not SchedulingTarget.Any:
            task = core_cluster_operations.get_task_from_table(cluster_id, task_id)
            return task.state
        else:
            task = core_cluster_operations.get_batch_task(cluster_id, task_id)
        return task.state

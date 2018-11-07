from aztk.models import SchedulingTarget

from .task_table import get_batch_task, get_task_from_table


def get_task(core_base_operations, id, task_id):
    """List all tasks on a job or cluster

    This will work for both Batch scheduling and scheduling_target

    Args:
        id: cluster or job id
    Returns:
        List[aztk.models.Task]

    """
    scheduling_target = core_base_operations.get_cluster_configuration(id).scheduling_target
    if scheduling_target is not SchedulingTarget.Any:
        return get_task_from_table(core_base_operations.table_service, id, task_id)
    else:
        return get_batch_task(core_base_operations.batch_client, id, task_id)

from azure.batch.models import BatchErrorException

from aztk.models import SchedulingTarget

from .task_table import list_batch_tasks, list_task_table_entries


def list_tasks(core_base_operations, id):
    """List all tasks on a job or cluster

    This works for both Batch scheduling and scheduling_target

    Args:
        id: cluster or job id
    Returns:
        List[aztk.models.Task]

    """
    scheduling_target = core_base_operations.get_cluster_configuration(id).scheduling_target
    if scheduling_target is not SchedulingTarget.Any:
        return list_task_table_entries(core_base_operations.table_service, id)
    else:
        return list_batch_tasks(batch_client=core_base_operations.batch_client, id=id)

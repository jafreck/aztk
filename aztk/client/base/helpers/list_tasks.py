from aztk.models import Task

from .get_recent_job import \
    get_recent_job  # TODO: move this function to azkt.client
from .task_table import list_task_table_entries


def list_tasks(core_base_operations, id):
    """List all tasks on a job or cluster

    This will work for both Batch scheduling and scheduling_target

    Args:
        id: cluster or job id
    Returns:
        List[aztk.models.Task] 

    """
    scheduling_target = core_base_operations.get_cluster_configuration(id).scheduling_target
    if scheduling_target:
        return list_task_table_entries(core_base_operations.table_service, id)
    else:
        # note: this currently only works for job_schedules
        # cluster impl is planned to move to job schedules
        recent_run_job = get_recent_job(core_base_operations, id)
        tasks = [
            Task(batch_task) for batch_task in core_base_operations.batch_client.tasks.list(job_id=recent_run_job.id)
        ]
        return tasks

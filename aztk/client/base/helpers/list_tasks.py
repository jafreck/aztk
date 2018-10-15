from aztk.models import SchedulingTarget, Task

from .get_recent_job import \
    get_recent_job  # TODO: move this function to azkt.client
from .task_table import list_task_table_entries


def __convert_batch_task_to_aztk_task(batch_task):
    task = Task()
    task.id = batch_task.id
    task.state = batch_task.state
    task.command_line = batch_task.command_line
    task.exit_code = batch_task.execution_info.exit_code
    task.start_time = batch_task.execution_info.start_time
    task.end_time = batch_task.execution_info.end_time
    task.failure_info = batch_task.execution_info.failure_info.message
    return task


def list_tasks(core_base_operations, id):
    """List all tasks on a job or cluster

    This will work for both Batch scheduling and scheduling_target

    Args:
        id: cluster or job id
    Returns:
        List[aztk.models.Task] 

    """
    scheduling_target = core_base_operations.get_cluster_configuration(id).scheduling_target
    if scheduling_target is not SchedulingTarget.Any:
        return list_task_table_entries(core_base_operations.table_service, id)
    else:
        # note: this currently only works for job_schedules
        # cluster impl is planned to move to job schedules
        recent_run_job = get_recent_job(core_base_operations, id)
        tasks = [
            __convert_batch_task_to_aztk_task(batch_task)
            for batch_task in core_base_operations.batch_client.task.list(job_id=recent_run_job.id)
        ]
        return tasks

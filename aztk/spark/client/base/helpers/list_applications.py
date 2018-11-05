from aztk.spark import models
from aztk.utils import batch_error_manager


def _list_applications(core_operations, id):
    # info about the app
    scheduling_target = core_operations.get_cluster_configuration(id).scheduling_target
    if scheduling_target is not models.SchedulingTarget.Any:
        return models.Application(core_operations.list_applications(id))

    recent_run_job = core_operations.get_recent_job(id)
    return core_operations.list_batch_tasks(id=recent_run_job.id)


def list_applications(core_operations, id):
    with batch_error_manager():
        return models.Application(_list_applications(core_operations, id))

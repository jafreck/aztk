from aztk import error
from aztk.spark import models
from aztk.utils import batch_error_manager


def _get_job(core_job_operations, job_id):
    job = core_job_operations.batch_client.job.get(job_id)
    tasks = [app for app in core_job_operations.list_tasks(id=job_id) if app.id != job_id]
    with batch_error_manager():
        pool = core_job_operations.batch_client.pool.get(job.execution_info.pool_id)
    pool = nodes = None

    if pool:
        nodes = core_job_operations.batch_client.compute_node.list(pool_id=pool.id)
    return job, tasks, pool, nodes


def get_job(core_job_operations, job_id):
    with batch_error_manager():
        job, tasks, pool, nodes = _get_job(core_job_operations, job_id)
        return models.Job(job, tasks, pool, nodes)

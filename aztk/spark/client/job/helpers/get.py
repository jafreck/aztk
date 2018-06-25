import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.utils import helpers

from .get_recent_job import get_recent_job


def _get_job(spark_client, job_id):
    job = spark_client.batch_client.job_schedule.get(job_id)
    job_apps = [
        app for app in spark_client.batch_client.task.list(job_id=job.execution_info.recent_job.id) if app.id != job_id
    ]
    recent_run_job = get_recent_job(spark_client, job_id)
    pool_prefix = recent_run_job.pool_info.auto_pool_specification.auto_pool_id_prefix
    pool = nodes = None
    for cloud_pool in spark_client.batch_client.pool.list():
        if pool_prefix in cloud_pool.id:
            pool = cloud_pool
            break
    if pool:
        nodes = spark_client.batch_client.compute_node.list(pool_id=pool.id)
    return job, job_apps, pool, nodes


def get_job(spark_job_client, job_id):
    try:
        job, apps, pool, nodes = _get_job(spark_job_client, job_id)
        return models.Job(job, apps, pool, nodes)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

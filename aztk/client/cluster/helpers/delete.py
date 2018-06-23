import azure.batch.models as batch_models


def delete_pool_and_job(cluster_client, pool_id: str, keep_logs: bool = False):
    """
        Delete a pool and it's associated job
        :param cluster_id: the pool to add the user to
        :return bool: deleted the pool if exists and job if exists
    """
    # job id is equal to pool id
    job_id = pool_id
    job_exists = True

    try:
        cluster_client.batch_client.job.get(job_id)
    except batch_models.batch_error.BatchErrorException:
        job_exists = False

    pool_exists = cluster_client.batch_client.pool.exists(pool_id)

    if job_exists:
        cluster_client.batch_client.job.delete(job_id)

    if pool_exists:
        cluster_client.batch_client.pool.delete(pool_id)

    if not keep_logs:
        cluster_data = cluster_client.get_cluster_data(pool_id)
        cluster_data.delete_container(pool_id)

    return job_exists or pool_exists

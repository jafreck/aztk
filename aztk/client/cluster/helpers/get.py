from aztk import error, models


def convert_job_id_to_pool_id(batch_client, cluster_id):
    job = batch_client.job.get(cluster_id)
    if job.execution_info and job.execution_info.pool_id:
        return job.execution_info.pool_id
    raise error.AztkError("No cluster with id {} does not exist.".format(cluster_id))


def get_pool_details(core_cluster_operations, cluster_id: str):
    """
        Print the information for the given cluster
        :param cluster_id: Id of the cluster
        :return pool: CloudPool, nodes: ComputeNodePaged
    """
    pool_id = convert_job_id_to_pool_id(core_cluster_operations.batch_client, cluster_id)
    pool = core_cluster_operations.batch_client.pool.get(pool_id)
    nodes = core_cluster_operations.batch_client.compute_node.list(pool_id=pool_id)
    return models.Cluster(cluster_id, pool, nodes)

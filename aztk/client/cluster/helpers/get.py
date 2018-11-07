import azure.batch.models as batch_models

from aztk import error, models


def convert_job_id_to_pool_id(batch_client, cluster_id):
    jobs = batch_client.pool.list(pool_list_options=batch_models.PoolListOptions(filter="id eq {}".format(cluster_id)))
    job = next(jobs)
    assert str.split('_', job.id)[0] == cluster_id
    print(job.pool_info.auto_pool_specification.pool.__dict__)
    if job.pool_info:
        return job.pool_info.pool_id
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
    return models.Cluster(pool, nodes)

# TODO: return Cluster instead of (pool, nodes)
from aztk import models
from aztk.utils import helpers


def get_pool_details(core_cluster_operations, cluster_id: str):
    """
        Print the information for the given cluster
        :param cluster_id: Id of the cluster
        :return pool: CloudPool, nodes: ComputeNodePaged
    """
    print(core_cluster_operations.get_task_table_entries(helpers.convert_id_to_table_id(cluster_id)))
    pool = core_cluster_operations.batch_client.pool.get(cluster_id)
    nodes = core_cluster_operations.batch_client.compute_node.list(pool_id=cluster_id)
    return models.Cluster(pool, nodes)

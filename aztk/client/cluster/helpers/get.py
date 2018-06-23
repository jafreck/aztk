

def get_pool_details(cluster_client, cluster_id: str):
    """
        Print the information for the given cluster
        :param cluster_id: Id of the cluster
        :return pool: CloudPool, nodes: ComputeNodePaged
    """
    pool = cluster_client.batch_client.pool.get(cluster_id)
    nodes = cluster_client.batch_client.compute_node.list(pool_id=cluster_id)
    return pool, nodes

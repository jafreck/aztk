from aztk.utils import batch_error_manager


def get_node(core_base_operations, cluster_id, node_id):
    with batch_error_manager():
        cluster = core_base_operations.get(cluster_id)
        return core_base_operations.batch_client.compute_node.get(cluster.pool.id, node_id)

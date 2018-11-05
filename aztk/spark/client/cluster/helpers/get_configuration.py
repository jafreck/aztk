from aztk.utils import batch_error_manager


def get_configuration(core_cluster_operations, cluster_id: str):
    with batch_error_manager():
        return core_cluster_operations.get_cluster_configuration(cluster_id)

from aztk.utils import batch_error_manager


def delete_cluster(core_cluster_operations, cluster_id: str, keep_logs: bool = False):
    with batch_error_manager():
        return core_cluster_operations.delete_batch_resources(cluster_id, keep_logs)

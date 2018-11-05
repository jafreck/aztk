from aztk.utils import batch_error_manager


def cluster_run(core_cluster_operations,
                cluster_id: str,
                command: str,
                host=False,
                internal: bool = False,
                timeout=None):
    with batch_error_manager():
        return core_cluster_operations.run(
            cluster_id, command, internal, container_name="spark" if not host else None, timeout=timeout)

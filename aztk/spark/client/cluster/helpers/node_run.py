from aztk.utils import batch_error_manager


def node_run(
        core_cluster_operations,
        cluster_id: str,
        node_id: str,
        command: str,
        host=False,
        internal: bool = False,
        timeout=None,
        block=False,
):
    with batch_error_manager():
        return core_cluster_operations.node_run(
            cluster_id,
            node_id,
            command,
            internal,
            container_name="spark" if not host else None,
            timeout=timeout,
            block=block)

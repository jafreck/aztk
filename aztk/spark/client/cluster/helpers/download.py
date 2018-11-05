from aztk.utils import batch_error_manager


def cluster_download(
        core_cluster_operations,
        cluster_id: str,
        source_path: str,
        destination_path: str = None,
        host: bool = False,
        internal: bool = False,
        timeout: int = None,
):
    with batch_error_manager():
        container_name = None if host else "spark"
        return core_cluster_operations.copy(
            cluster_id,
            source_path,
            destination_path=destination_path,
            container_name=container_name,
            get=True,
            internal=internal,
            timeout=timeout,
        )

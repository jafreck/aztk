from aztk.spark import models
from aztk.utils import batch_error_manager


def get_cluster(core_cluster_operations, cluster_id: str):
    with batch_error_manager():
        cluster = core_cluster_operations.get(cluster_id)
        return models.Cluster(cluster)

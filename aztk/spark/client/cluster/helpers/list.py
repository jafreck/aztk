from aztk import models as base_models
from aztk.spark import models
from aztk.utils import batch_error_manager


def list_clusters(core_cluster_operations):
    with batch_error_manager():
        software_metadata_key = base_models.Software.spark
        return [models.Cluster(cluster) for cluster in core_cluster_operations.list(software_metadata_key)]

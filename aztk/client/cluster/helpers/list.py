from aztk import models
from aztk.utils import constants


def job_get_pool(core_cluster_operations, job):
    if job.execution_info and job.execution_info.pool_id:
        return core_cluster_operations.batch_client.pool.get(job.execution_info.pool_id)


def list_clusters(core_cluster_operations, software_metadata_key):
    """
        List all the cluster on your account.
    """
    jobs = core_cluster_operations.batch_client.job.list()
    software_metadata = (constants.AZTK_SOFTWARE_METADATA_KEY, software_metadata_key)
    cluster_metadata = (constants.AZTK_MODE_METADATA_KEY, constants.AZTK_CLUSTER_MODE_METADATA)

    aztk_clusters = []
    for job in jobs:
        if job.metadata:
            job_metadata = [(metadata.name, metadata.value) for metadata in job.metadata]
            if all([metadata in job_metadata for metadata in [software_metadata, cluster_metadata]]):
                aztk_clusters.append(models.Cluster(job.id, job_get_pool(core_cluster_operations, job)))
    return aztk_clusters

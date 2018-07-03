import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.utils import helpers


def cluster_copy(spark_cluster_operations, cluster_id: str, source_path: str, destination_path: str, host: bool = False, internal: bool = False, timeout: int = None):
    try:
        container_name = None if host else 'spark'
        return super(type(spark_cluster_operations), spark_cluster_operations).copy(
            cluster_id,
            source_path,
            destination_path=destination_path,
            container_name=container_name,
            get=False,
            internal=internal,
            timeout=timeout)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

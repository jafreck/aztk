import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import helpers


def cluster_run(spark_cluster_operations, cluster_id: str, command: str, host=False, internal: bool = False, timeout=None):
    try:
        return super(type(spark_cluster_operations), spark_cluster_operations).run(
            cluster_id, command, internal, container_name='spark' if not host else None, timeout=timeout)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

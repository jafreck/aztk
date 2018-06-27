import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.utils import helpers


def delete_cluster(spark_cluster_client, cluster_id: str, keep_logs: bool = False):
    try:
        return super(type(spark_cluster_client), spark_cluster_client).delete(cluster_id, keep_logs)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

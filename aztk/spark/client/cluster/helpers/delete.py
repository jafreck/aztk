from aztk import error
import azure.batch.models.batch_error as batch_error
from aztk.spark import helpers


def delete_cluster(spark_cluster_client, cluster_id: str, keep_logs: bool = False):
    try:
        return spark_cluster_client.delete_pool_and_job(cluster_id, keep_logs)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

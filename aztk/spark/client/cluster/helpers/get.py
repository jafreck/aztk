import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.spark import helpers


def get_cluster(spark_cluster_client, cluster_id: str):
    try:
        pool, nodes = spark_cluster_client.get_pool_details(cluster_id)
        return models.Cluster(pool, nodes)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

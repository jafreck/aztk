import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import helpers


def create_user(spark_cluster_client, cluster_id: str, username: str, password: str = None, ssh_key: str = None) -> str:
    try:
        cluster = spark_cluster_client.get_cluster(cluster_id)
        master_node_id = cluster.master_node_id
        if not master_node_id:
            raise error.ClusterNotReadyError("The master has not yet been picked, a user cannot be added.")
        spark_cluster_client.create_user_on_pool(username, cluster.id, cluster.nodes, ssh_key, password)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

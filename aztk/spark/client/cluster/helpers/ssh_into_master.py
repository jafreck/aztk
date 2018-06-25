
import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import helpers


def cluster_ssh_into_master(spark_cluster_client, cluster_id, node_id, username, ssh_key=None, password=None, port_forward_list=None, internal=False):
    try:
        spark_cluster_client.ssh_into_node(cluster_id, node_id, username, ssh_key, password, port_forward_list, internal)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))

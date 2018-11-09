from aztk import models
from aztk.utils import batch_error_manager


def _get_remote_login_settings(core_base_operations, cluster_id: str, node_id: str):
    """
    Get the remote_login_settings for node
    :param cluster_id
    :param node_id
    :returns aztk.models.RemoteLogin
    """
    cluster = core_base_operations.get(cluster_id)
    result = core_base_operations.batch_client.compute_node.get_remote_login_settings(cluster.pool.id, node_id)
    return models.RemoteLogin(ip_address=result.remote_login_ip_address, port=str(result.remote_login_port))


def get_remote_login_settings(core_base_operations, cluster_id: str, node_id: str):
    with batch_error_manager():
        return _get_remote_login_settings(core_base_operations, cluster_id, node_id)

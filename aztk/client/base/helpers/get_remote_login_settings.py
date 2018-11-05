from aztk import models
from aztk.utils import batch_error_manager


def _get_remote_login_settings(base_client, pool_id: str, node_id: str):
    """
    Get the remote_login_settings for node
    :param pool_id
    :param node_id
    :returns aztk.models.RemoteLogin
    """
    result = base_client.batch_client.compute_node.get_remote_login_settings(pool_id, node_id)
    return models.RemoteLogin(ip_address=result.remote_login_ip_address, port=str(result.remote_login_port))


def get_remote_login_settings(base_client, cluster_id: str, node_id: str):
    with batch_error_manager():
        return _get_remote_login_settings(base_client, cluster_id, node_id)

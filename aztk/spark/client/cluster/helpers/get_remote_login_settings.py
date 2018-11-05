from aztk.spark import models
from aztk.utils import batch_error_manager


def get_remote_login_settings(core_cluster_operations, id: str, node_id: str):
    with batch_error_manager():
        return models.RemoteLogin(core_cluster_operations.get_remote_login_settings(id, node_id))

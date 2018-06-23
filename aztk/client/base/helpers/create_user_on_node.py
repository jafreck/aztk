import datetime
from datetime import datetime

import azure.batch.models as batch_models
import azure.batch.models.batch_error as batch_error

from aztk import models
from aztk.utils import get_ssh_key


def __create_user(self, pool_id: str, node_id: str, username: str, password: str = None, ssh_key: str = None) -> str:
    """
        Create a pool user
        :param pool: the pool to add the user to
        :param node: the node to add the user to
        :param username: username of the user to add
        :param password: password of the user to add
        :param ssh_key: ssh_key of the user to add
    """
    # Create new ssh user for the given node
    self.batch_client.compute_node.add_user(
        pool_id,
        node_id,
        batch_models.ComputeNodeUser(
            name=username,
            is_admin=True,
            password=password,
            ssh_public_key=get_ssh_key.get_user_public_key(ssh_key, self.secrets_config),
            expiry_time=datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365),
        ),
    )


def create_user_on_node(base_client, username, pool_id, node_id, ssh_key=None, password=None):
    try:
        __create_user(base_client, pool_id=pool_id, node_id=node_id, username=username, ssh_key=ssh_key, password=password)
    except batch_error.BatchErrorException as error:
        try:
            base_client.__delete_user(pool_id, node_id, username)
            base_client.__create_user(pool_id=pool_id, node_id=node_id, username=username, ssh_key=ssh_key)
        except batch_error.BatchErrorException as error:
            raise error

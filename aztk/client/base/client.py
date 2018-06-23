import aztk.models as models
from aztk.internal import cluster_data
from aztk.utils import ssh as ssh_lib
from aztk.utils import azure_api

from .helpers import (create_user_on_node, create_user_on_pool,
                      delete_user_on_node, generate_user_on_node,
                      generate_user_on_pool, ssh_into_node)


class BaseClient:
    '''
        Base client that all other clients inherit from
    '''

    def __init__(self, secrets_config: models.SecretsConfiguration):
        self.secrets_config = secrets_config

        azure_api.validate_secrets(secrets_config)
        self.batch_client = azure_api.make_batch_client(secrets_config)
        self.blob_client = azure_api.make_blob_client(secrets_config)

    def get_cluster_config(self, cluster_id: str) -> models.ClusterConfiguration:
        return self.get_cluster_data(cluster_id).read_cluster_config()

    def get_cluster_data(self, cluster_id: str) -> cluster_data.ClusterData:
        """
        Returns ClusterData object to manage data related to the given cluster id
        """
        return cluster_data.ClusterData(self.blob_client, cluster_id)

    def ssh_into_node(self,
                      pool_id,
                      node_id,
                      username,
                      ssh_key=None,
                      password=None,
                      port_forward_list=None,
                      internal=False):
        '''
        Opens a ssh tunnel to the node for port forwarding
        '''
        ssh_into_node.ssh_into_node(self, pool_id, node_id, username, ssh_key, password, port_forward_list, internal)

    def create_user_on_node(self, username, pool_id, node_id, ssh_key=None, password=None):
        return create_user_on_node.create_user_on_node(self, username, pool_id, node_id, ssh_key, password)

    def create_user_on_pool(self, username, pool_id, nodes, ssh_pub_key=None, password=None):
        return create_user_on_pool.create_user_on_pool(self, username, pool_id, nodes, ssh_pub_key, password)

    def generate_user_on_node(self, pool_id, node_id):
        return generate_user_on_node.generate_user_on_node(self, pool_id, node_id)

    def generate_user_on_pool(self, pool_id, nodes):
        return generate_user_on_pool.generate_user_on_pool(self, pool_id, nodes)

    def delete_user_on_node(self, pool_id: str, node_id: str, username: str) -> str:
        return delete_user_on_node.delete_user(self, pool_id, node_id, username)

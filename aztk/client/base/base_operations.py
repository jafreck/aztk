from aztk import models
from aztk.internal import cluster_data
from aztk.utils import ssh as ssh_lib

from .helpers import (create_user_on_node, create_user_on_pool,
                      delete_user_on_node, delete_user_on_pool,
                      generate_user_on_node, generate_user_on_pool,
                      get_application_log, get_remote_login_settings, node_run,
                      run, ssh_into_node)


class BaseOperations:
    '''
        Base operations that all other operations inherit from
    '''

    def __init__(self, context):
        self.batch_client = context['batch_client']
        self.blob_client = context['blob_client']
        self.secrets_configuration = context['secrets_configuration']

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

    def delete_user_on_pool(self, username, pool_id, nodes):    #TODO: change from pool_id, nodes to cluster_id
        return delete_user_on_pool.delete_user_on_pool(self, username, pool_id, nodes)

    def node_run(self, cluster_id, node_id, command, internal, container_name=None, timeout=None):
        return node_run.node_run(self, cluster_id, node_id, command, internal, container_name, timeout)

    def get_remote_login_settings(self, cluster_id: str, node_id: str):
        return get_remote_login_settings.get_remote_login_settings(self, cluster_id, node_id)

    def run(self, cluster_id, command, internal, container_name=None, timeout=None):
        return run.cluster_run(self, cluster_id, command, internal, container_name, timeout)

    def get_application_log(self, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
        return get_application_log.get_application_log(self, cluster_id, application_name, tail, current_bytes)

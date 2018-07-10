from aztk import models
from aztk.internal import cluster_data
from aztk.utils import ssh as ssh_lib

from .helpers import (create_user_on_node, create_user_on_pool, delete_user_on_node, delete_user_on_pool,
                      generate_user_on_node, generate_user_on_pool, get_application_log, get_remote_login_settings,
                      node_run, run, ssh_into_node)


class BaseOperations:
    """Base operations that all other operations inherit from

    Attributes:
        batch_client (:obj:`azure.batch.batch_service_client.BatchServiceClient`): Client used to interact with the Azure Batch service.
        blob_client (:obj:`azure.storage.blob.BlockBlobService`):  Client used to interact with the Azure Storage Blob service.
        secrets_configuration (:obj:`aztk.models.SecretsConfiguration`): Model that holds AZTK secrets used to authenticate with Azure and the clusters.
    """

    def __init__(self, context):
        self.batch_client = context['batch_client']
        self.blob_client = context['blob_client']
        self.secrets_configuration = context['secrets_configuration']

    def get_cluster_config(self, cluster_id: str) -> models.ClusterConfiguration:
        """Open an ssh tunnel to a node

        Args:
            pool_id (:obj:`str`): the id of the cluster the node is in
            node_id (:obj:`str`): the id of the node to open the ssh tunnel to
            username (:obj:`str`): the username to authenticate the ssh session
            ssh_key (:obj:`str`, optional): ssh public key to create the user with, must use ssh_key or password. Defaults to None.
            password (:obj:`str`, optional): password for the user, must use ssh_key or password. Defaults to None.
            port_forward_list (:obj:`List[PortForwardingSpecification`, optional): list of PortForwardingSpecifications.
                The defined ports will be forwarded to the client.
            internal (:obj:`bool`, optional): if True, this will connect to the node using its internal IP.
                Only use this if running within the same VNET as the cluster. Defaults to False.
        Returns:
            ClusterConfiguration: Object representing the cluster's configuration
        """
        return self.get_cluster_data(cluster_id).read_cluster_config()

    def get_cluster_data(self, cluster_id: str) -> cluster_data.ClusterData:
        """Gets the ClusterData object to manage data related to the given cluster

        Args:
            cluster_id (:obj:`str`): the id of the cluster to get

        Returns:
            ClusterData: Object used to manage the data and storage functions for a cluster
        """
        return cluster_data.ClusterData(self.blob_client, cluster_id)

    #TODO: rename pool to cluster
    def ssh_into_node(self,
                      pool_id,
                      node_id,
                      username,
                      ssh_key=None,
                      password=None,
                      port_forward_list=None,
                      internal=False):
        """Open an ssh tunnel to a node

        Args:
            pool_id (:obj:`str`): the id of the cluster the node is in
            node_id (:obj:`str`): the id of the node to open the ssh tunnel to
            username (:obj:`str`): the username to authenticate the ssh session
            ssh_key (:obj:`str`, optional): ssh public key to create the user with, must use ssh_key or password. Defaults to None.
            password (:obj:`str`, optional): password for the user, must use ssh_key or password. Defaults to None.
            port_forward_list (:obj:`List[PortForwardingSpecification`, optional): list of PortForwardingSpecifications.
                The defined ports will be forwarded to the client.
            internal (:obj:`bool`, optional): if True, this will connect to the node using its internal IP.
                Only use this if running within the same VNET as the cluster. Defaults to False.
        Returns:
            None
        """
        ssh_into_node.ssh_into_node(self, pool_id, node_id, username, ssh_key, password, port_forward_list, internal)

    #TODO: rename pool to cluster
    def create_user_on_node(self, username, pool_id, node_id, ssh_key=None, password=None):
        """Create a user on a node

        Args:
            username (:obj:`str`): name of the user to create.
            pool_id (:obj:`str`): id of the cluster to create the user on.
            node_id (:obj:`str`): id of the node in the cluster to create the user on.
            ssh_key (:obj:`str`, optional): ssh public key to create the user with, must use ssh_key or password.
            password (:obj:`str`, optional): password for the user, must use ssh_key or password.
        """
        return create_user_on_node.create_user_on_node(self, username, pool_id, node_id, ssh_key, password)

    #TODO: rename pool to cluster
    def create_user_on_pool(self, username, pool_id, nodes, ssh_pub_key=None, password=None):
        """Create a user on every node in the cluster

        Args:
            username (:obj:`str`): name of the user to create.
            pool_id (:obj:`str`): id of the cluster to create the user on.
            node_id (:obj:`str`): id of the node in the cluster to create the user on.
            ssh_key (:obj:`str`, optional): ssh public key to create the user with, must use ssh_key or password. Defaults to None.
            password (:obj:`str`, optional): password for the user, must use ssh_key or password. Defaults to None.

        Returns:
            None
        """
        return create_user_on_pool.create_user_on_pool(self, username, pool_id, nodes, ssh_pub_key, password)

    #TODO: rename pool to cluster
    def generate_user_on_node(self, pool_id, node_id):
        """Create a user with an autogenerated username and ssh_key on the given node.

        Args:
            pool_id (:obj:`str`): the id of the cluster to generate the user on.
            node_id (:obj:`str`): the id of the node in the cluster to generate the user on.

        Returns:
            tuple: A tuple of the form (username (:obj:`str`), ssh_key) where ssh_key is a Cryptodome.RSA key.
        """
        return generate_user_on_node.generate_user_on_node(self, pool_id, node_id)

    #TODO: rename pool to cluster
    def generate_user_on_pool(self, pool_id, nodes):
        """Create a user with an autogenerated username and ssh_key on the cluster

        Args:
            pool_id (:obj:`str`): the id of the cluster to generate the user on.
            node_id (:obj:`str`): the id of the node in the cluster to generate the user on.

        Returns:
            tuple: A tuple of the form (username (:obj:`str`), ssh_key) where ssh_key is a Cryptodome.RSA key.
        """
        return generate_user_on_pool.generate_user_on_pool(self, pool_id, nodes)

    #TODO: rename pool to cluster
    def delete_user_on_node(self, pool_id: str, node_id: str, username: str) -> str:
        """Delete a user on a node

        Args:
            pool_id (:obj:`str`): the id of the cluster to delete the user on.
            node_id (:obj:`str`): the id of the node in the cluster to delete the user on.
            username (:obj:`str`): the name of the user to delete.

        Returns:
            None
        """
        return delete_user_on_node.delete_user(self, pool_id, node_id, username)

    #TODO: rename pool to cluster
    def delete_user_on_pool(self, username, pool_id, nodes):
        """Delete a user on every node in the cluster

        Args:
            pool_id (:obj:`str`): the id of the cluster to delete the user on.
            node_id (:obj:`str`): the id of the node in the cluster to delete the user on.
            username (:obj:`str`): the name of the user to delete.

        Returns:
            None
        """
        return delete_user_on_pool.delete_user_on_pool(self, username, pool_id, nodes)

    def node_run(self, cluster_id, node_id, command, internal, container_name=None, timeout=None):
        """Run a bash command on the given node

        Args:
            cluster_id (:obj:`str`): the id of the cluster to run the command on.
            node_id (:obj:`str`): the id of the node in the cluster to run the command on.
            command (:obj:`str`): the bash command to execute on the node.
            internal (:obj:`bool`): if True, this will connect to the node using its internal IP.
                Only use this if running within the same VNET as the cluster. Defaults to False.
            container_name=None (:obj:`str`, optional): the name of the container to run the command in.
                If None, the command will run on the host VM. Defaults to None.
            timeout=None (:obj:`str`, optional): The timeout in seconds for establishing a connection to the node.
                Defaults to None.

        Returns:
            NodeOutput:
        """
        return node_run.node_run(self, cluster_id, node_id, command, internal, container_name, timeout)

    def get_remote_login_settings(self, id: str, node_id: str):
        """Get the remote login information for a node in a cluster

        Args:
            id (:obj:`str`): the id of the cluster the node is in
            node_id (:obj:`str`): the id of the node in the cluster

        Returns:
            RemoteLogin:
        """
        return get_remote_login_settings.get_remote_login_settings(self, id, node_id)

    def run(self, cluster_id, command, internal, container_name=None, timeout=None):
        """Run a bash command on every node in the cluster

        Args:
            cluster_id (:obj:`str`): the id of the cluster to run the command on.
            command (:obj:`str`): the bash command to execute on the node.
            internal (:obj:`bool`): if true, this will connect to the node using its internal IP.
                Only use this if running within the same VNET as the cluster. Defaults to False.
            container_name=None (:obj:`str`, optional): the name of the container to run the command in.
                If None, the command will run on the host VM. Defaults to None.
            timeout=None (:obj:`str`, optional): The timeout in seconds for establishing a connection to the node.
                Defaults to None.

        Returns:
            List[NodeOutput]
        """
        return run.cluster_run(self, cluster_id, command, internal, container_name, timeout)

    def get_application_log(self, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
        """Get the log for a running or completed application

        Args:
            cluster_id (:obj:`str`): the id of the cluster to run the command on.
            application_name (:obj:`str`): str
            tail (:obj:`bool`, optional): If True, get the remaining bytes after current_bytes. Otherwise, the whole log will be retrieved.
                Only use this if streaming the log as it is being written. Defaults to False.
            current_bytes (:obj:`int`): Specifies the last seen byte, so only the bytes after current_bytes are retrieved.
                Only useful is streaming the log as it is being written. Only used if tail is True.

        Returns:
            aztk.models.ApplicationLog: a model representing the output of the application.
        """
        return get_application_log.get_application_log(self, cluster_id, application_name, tail, current_bytes)

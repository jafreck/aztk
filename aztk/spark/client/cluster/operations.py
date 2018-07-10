from aztk.client.cluster import CoreClusterOperations
from aztk.spark import models
from aztk.spark.client.base import SparkBaseOperations

from .helpers import (copy, create, create_user, delete, diagnostics, download,
                      get, get_application_status, list, node_run, run, submit)


class ClusterOperations(CoreClusterOperations, SparkBaseOperations):
    def create(self, cluster_configuration: models.ClusterConfiguration, wait: bool = False):
        """Create a cluster.

        Args:
            cluster_configuration (:obj:`ClusterConfiguration`): Configuration for the cluster to be created.
            wait (:obj:`bool`): if True, this function will block until the cluster creation is finished.

        Returns:
            Cluster: An aztk.models.Cluster object representing the state and configuration of the cluster.
        """
        return create.create_cluster(self, cluster_configuration, wait)

    def delete(self, id: str, keep_logs: bool = False):
        """Delete a cluster.

        Args:
            id (:obj:`str`): the id of the cluster to delete.
            keep_logs (:obj:`bool`): if True, this function will block until the cluster creation is finished.

        Returns:
            True if the deletion process was successful.
        """
        return delete.delete_cluster(self, id, keep_logs)

    def get(self, id: str):
        """Get details about the state of a cluster.

        Args:
            id (:obj:`str`): the id of the cluster to get.

        Returns:
            Cluster: An aztk.models.Cluster object representing the state and configuration of the cluster.
        """
        return get.get_cluster(self, id)

    def list(self):
        """List all clusters.

        Returns:
            List[Cluster]: List of aztk.models.Cluster objects each representing the state and configuration of the cluster.
        """
        return list.list_clusters(self)

    def submit(self, id: str, application: models.ApplicationConfiguration, remote: bool = False, wait: bool = False):
        """Submit an application to a cluster.

        Args:
            id (:obj:`str`): the id of the cluster to submit the application to.
            application (:obj:`aztk.spark.models.ApplicationConfiguration`): Application definition
            remote (:obj:`bool`, optional): Defaults to False.
            wait (:obj:`bool`, optional): If True, this function blocks until the application has completed. Defaults to False.

        Returns:
            Cluster: An aztk.models.Cluster object representing the state and configuration of the cluster.
        """
        return submit.submit(self, id, application, remote, wait)

    def create_user(self, id: str, username: str, password: str = None, ssh_key: str = None):
        return create_user.create_user(self, id, username, ssh_key, password)

    def get_application_status(self, id: str, application_name: str):
        return get_application_status.get_application_status(self, id, application_name)

    def run(self, id: str, command: str, host=False, internal: bool = False, timeout=None):
        return run.cluster_run(self, id, command, host, internal, timeout)

    def node_run(self, id: str, node_id: str, command: str, host=False, internal: bool = False, timeout=None):
        return node_run.node_run(self, id, node_id, command, host, internal, timeout)

    def copy(self,
             id: str,
             source_path: str,
             destination_path: str,
             host: bool = False,
             internal: bool = False,
             timeout: int = None):
        return copy.cluster_copy(self, id, source_path, destination_path, host, internal, timeout)

    def download(self,
                 id: str,
                 source_path: str,
                 destination_path: str = None,
                 host: bool = False,
                 internal: bool = False,
                 timeout: int = None):
        return download.cluster_download(self, id, source_path, destination_path, host, internal, timeout)

    def diagnostics(self, id, output_directory=None):
        return diagnostics.run_cluster_diagnostics(self, id, output_directory)

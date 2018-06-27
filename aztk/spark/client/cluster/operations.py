from aztk.spark import models
from aztk.spark.client.base import SparkBaseOperations
from aztk.client.cluster import CoreClusterOperations

from .helpers import (copy, create, create_user, delete, get, get_application_log, get_application_status, list,
                      node_run, run, submit, diagnostics)


class ClusterOperations(CoreClusterOperations, SparkBaseOperations):
    def create(self, cluster_configuration: models.ClusterConfiguration, wait: bool = False):
        return create.create_cluster(self, cluster_configuration, wait)

    def delete(self, cluster_id: str, keep_logs: bool = False):
        return delete.delete_cluster(self, cluster_id, keep_logs)

    def get(self, cluster_id: str):
        return get.get_cluster(self, cluster_id)

    def list(self):
        return list.list_clusters(self)

    def submit(self,
               cluster_id: str,
               application: models.ApplicationConfiguration,
               remote: bool = False,
               wait: bool = False):
        return submit.submit(self, cluster_id, application, remote, wait)

    def create_user(self, cluster_id: str, username: str, password: str = None, ssh_key: str = None):
        return create_user.create_user(self, cluster_id, username, ssh_key, password)

    def get_application_log(self, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
        return get_application_log.get_application_log(self, cluster_id, application_name, tail, current_bytes)

    def get_application_status(self, cluster_id: str, application_name: str):
        return get_application_status.get_application_status(self, cluster_id, application_name)

    def run(self, cluster_id: str, command: str, host=False, internal: bool = False, timeout=None):
        return run.cluster_run(self, cluster_id, command, host, internal, timeout)

    def node_run(self, cluster_id: str, node_id: str, command: str, host=False, internal: bool = False, timeout=None):
        return node_run.node_run(self, cluster_id, node_id, command, host, internal, timeout)

    def copy(self,
             cluster_id: str,
             source_path: str,
             destination_path: str,
             host: bool = False,
             internal: bool = False,
             timeout: int = None):
        return copy.cluster_copy(self, cluster_id, source_path, destination_path, host, internal, timeout)

    def diagnostics(self, cluster_id, output_directory=None):
        return diagnostics.run_cluster_diagnostics(self, cluster_id, output_directory)

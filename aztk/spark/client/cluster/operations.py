from aztk.client.cluster import CoreClusterOperations
from aztk.spark import models
from aztk.spark.client.base import SparkBaseOperations

from .helpers import (copy, create, create_user, delete, diagnostics, download, get, get_application_log,
                      get_application_status, list, node_run, run, submit)


class ClusterOperations(CoreClusterOperations, SparkBaseOperations):
    def create(self, cluster_configuration: models.ClusterConfiguration, wait: bool = False):
        return create.create_cluster(self, cluster_configuration, wait)

    def delete(self, id: str, keep_logs: bool = False):
        return delete.delete_cluster(self, id, keep_logs)

    def get(self, id: str):
        return get.get_cluster(self, id)

    def list(self):
        return list.list_clusters(self)

    def submit(self,
               id: str,
               application: models.ApplicationConfiguration,
               remote: bool = False,
               wait: bool = False):
        return submit.submit(self, id, application, remote, wait)

    def create_user(self, id: str, username: str, password: str = None, ssh_key: str = None):
        return create_user.create_user(self, id, username, ssh_key, password)

    def get_application_log(self, id: str, application_name: str, tail=False, current_bytes: int = 0):
        return get_application_log.get_application_log(self, id, application_name, tail, current_bytes)

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
